import logging
import signal
import asyncio
from functools import partial
from collections import defaultdict, namedtuple
from again.utils import natural_sort
import time

from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol
from .pinger import TCPPinger, HTTPPinger
from .utils.log import setup_logging

import json
import ssl

Service = namedtuple('Service', ['name', 'version', 'dependencies', 'host', 'port', 'node_id', 'type'])


def tree():
    return defaultdict(tree)


def json_file_to_dict(_file: str) -> dict:
    config = None
    with open(_file) as config_file:
        config = json.load(config_file)

    return config


class Repository:

    def __init__(self):
        self._registered_services = defaultdict(lambda: defaultdict(list))
        self._pending_services = defaultdict(list)
        self._service_dependencies = {}
        self._subscribe_list = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self._uptimes = tree()
        self.logger = logging.getLogger(__name__)

    def register_service(self, service: Service):
        service_name = self._get_full_service_name(service.name, service.version)
        service_entry = (service.host, service.port, service.node_id, service.type)
        self._registered_services[service.name][service.version].append(service_entry)
        self._pending_services[service_name].append(service.node_id)
        self._uptimes[service_name][service.host] = {
            'uptime': int(time.time()),
            'node_id': service.node_id
        }

        if len(service.dependencies):
            if self._service_dependencies.get(service_name) is None:
                self._service_dependencies[service_name] = service.dependencies

    def is_pending(self, service, version):
        return self._get_full_service_name(service, version) in self._pending_services

    def add_pending_service(self, service, version, node_id):
        self._pending_services[self._get_full_service_name(service, version)].append(node_id)

    def get_pending_services(self):
        return [self._split_key(k) for k in self._pending_services.keys()]

    def get_pending_instances(self, service, version):
        return self._pending_services.get(self._get_full_service_name(service, version), [])

    def remove_pending_instance(self, service, version, node_id):
        self.get_pending_instances(service, version).remove(node_id)
        if not len(self.get_pending_instances(service, version)):
            self._pending_services.pop(self._get_full_service_name(service, version))

    def get_instances(self, service, version):
        return self._registered_services[service][version]

    def get_versioned_instances(self, service, version):
        version = self._get_non_breaking_version(version, list(self._registered_services[service].keys()))
        return self._registered_services[service][version]

    def get_consumers(self, service_name, service_version):
        consumers = set()
        for service, vendors in self._service_dependencies.items():
            for each in vendors:
                if each['service'] == service_name and each['version'] == service_version:
                    consumers.add(self._split_key(service))
        return consumers

    def get_vendors(self, service, version):
        return self._service_dependencies.get(self._get_full_service_name(service, version), [])

    def get_node(self, node_id):
        for name, versions in self._registered_services.items():
            for version, instances in versions.items():
                for host, port, node, service_type in instances:
                    if node_id == node:
                        return Service(name, version, [], host, port, node, service_type)
        return None

    def remove_node(self, node_id):
        thehost = None
        for name, versions in self._registered_services.items():
            for version, instances in versions.items():
                for instance in instances:
                    host, port, node, service_type = instance
                    if node_id == node:
                        thehost = host
                        instances.remove(instance)
                        break
        for name, nodes in self._uptimes.items():
            for host, uptimes in nodes.items():
                if host == thehost and uptimes['node_id'] == node_id:
                    uptimes['downtime'] = int(time.time())
                    self.log_uptimes()
        return None

    def get_uptimes(self):
        return self._uptimes

    def log_uptimes(self):
        for name, nodes in self._uptimes.items():
            for host, d in nodes.items():
                now = int(time.time())
                live = d.get('downtime', 0) < d['uptime']
                uptime = now - d['uptime'] if live else 0
                logd = {'service_name': name.split('/')[0], 'hostname': host, 'status': live,
                        'uptime': int(uptime)}
                logging.getLogger('stats').info(logd)

    def xsubscribe(self, service, version, host, port, node_id, endpoints):
        entry = (service, version, host, port, node_id)
        for endpoint in endpoints:
            self._subscribe_list[endpoint['service']][endpoint['version']][endpoint['endpoint']].append(
                entry + (endpoint['strategy'],))

    def get_subscribers(self, service, version, endpoint):
        return self._subscribe_list[service][version][endpoint]

    def _get_non_breaking_version(self, version, versions):
        if version in versions:
            return version
        versions.sort(key=natural_sort, reverse=True)
        for v in versions:
            if self._is_non_breaking(v, version):
                return v
        return version

    @staticmethod
    def _is_non_breaking(v, version):
        return version.split('.')[0] == v.split('.')[0]

    @staticmethod
    def _get_full_service_name(service: str, version):
        return '{}/{}'.format(service, version)

    @staticmethod
    def _split_key(key: str):
        return tuple(key.split('/'))


class Registry:

    def __init__(self, ip, port, repository: Repository):
        self._ip = ip
        self._port = port
        self._loop = asyncio.get_event_loop()
        self._client_protocols = {}
        self._service_protocols = {}
        self._repository = repository
        self._tcp_pingers = {}
        self._http_pingers = {}
        self.logger = logging.getLogger()
        try:
            config = json_file_to_dict('./config.json')
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self._ssl_context.load_cert_chain(config['SSL_CERTIFICATE'], config['SSL_KEY'])
        except:
            self._ssl_context = None

    def start(self):
        setup_logging("registry")
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))
        registry_coroutine = self._loop.create_server(
            partial(get_vyked_protocol, self), self._ip, self._port, ssl=self._ssl_context)
        server = self._loop.run_until_complete(registry_coroutine)
        try:
            self._loop.run_forever()
        except Exception as e:
            print(e)
        finally:
            server.close()
            self._loop.run_until_complete(server.wait_closed())
            self._loop.close()

    def _stop(self, signame: str):
        print('\ngot signal {} - exiting'.format(signame))
        self._loop.stop()

    def receive(self, packet: dict, protocol, transport):
        request_type = packet['type']
        if request_type in ['register', 'get_instances', 'xsubscribe', 'get_subscribers']:
            for_log = {}
            params = packet['params']
            for_log["caller_name"] = params['service'] + '/' + params['version']
            for_log["caller_address"] = transport.get_extra_info("peername")[0]
            for_log["request_type"] = request_type
            self.logger.debug(for_log)
        if request_type == 'register':
            self.register_service(packet, protocol)
        elif request_type == 'get_instances':
            self.get_service_instances(packet, protocol)
        elif request_type == 'xsubscribe':
            self._xsubscribe(packet)
        elif request_type == 'get_subscribers':
            self.get_subscribers(packet, protocol)
        elif request_type == 'pong':
            self._ping(packet)
        elif request_type == 'ping':
            self._pong(packet, protocol)
        elif request_type == 'uptime_report':
            self._get_uptime_report(packet, protocol)

    def deregister_service(self, host, port, node_id):
        service = self._repository.get_node(node_id)
        self._tcp_pingers.pop(node_id, None)
        self._http_pingers.pop((host, port), None)
        if service:
            for_log = {"caller_name": service.name + '/' + service.version, "caller_address": service.host,
                       "request_type": 'deregister'}
            self.logger.debug(for_log)
            self._repository.remove_node(node_id)
            if service is not None:
                self._service_protocols.pop(node_id, None)
                self._client_protocols.pop(node_id, None)
                self._notify_consumers(service.name, service.version, node_id)
                if not len(self._repository.get_instances(service.name, service.version)):
                    consumers = self._repository.get_consumers(service.name, service.version)
                    for consumer_name, consumer_version in consumers:
                        for _, _, node_id, _ in self._repository.get_instances(consumer_name, consumer_version):
                            self._repository.add_pending_service(consumer_name, consumer_version, node_id)

    def register_service(self, packet: dict, registry_protocol):
        params = packet['params']
        service = Service(params['service'], params['version'], params['dependencies'], params['host'], params['port'],
                          params['node_id'], params['type'])
        self._repository.register_service(service)
        self._client_protocols[params['node_id']] = registry_protocol
        self._connect_to_service(params['host'], params['port'], params['node_id'], params['type'])
        self._handle_pending_registrations()
        self._inform_consumers(service)

    def _inform_consumers(self, service: Service):
        consumers = self._repository.get_consumers(service.name, service.version)
        for service_name, service_version in consumers:
            if not self._repository.is_pending(service_name, service_version):
                instances = self._repository.get_instances(service_name, service_version)
                for host, port, node, type in instances:
                    protocol = self._client_protocols[node]
                    protocol.send(ControlPacket.new_instance(
                        service.name, service.version, service.host, service.port, service.node_id, service.type))

    def _send_activated_packet(self, service, version, node):
        protocol = self._client_protocols.get(node, None)
        if protocol:
            packet = self._make_activated_packet(service, version)
            protocol.send(packet)

    def _handle_pending_registrations(self):
        for service, version in self._repository.get_pending_services():
            vendors = self._repository.get_vendors(service, version)
            should_activate = True
            for vendor in vendors:
                instances = self._repository.get_versioned_instances(vendor['service'], vendor['version'])
                tcp_instances = [instance for instance in instances if instance[3] == 'tcp']
                if not len(tcp_instances):
                    should_activate = False
                    break
            for node in self._repository.get_pending_instances(service, version):
                if should_activate:
                    self._send_activated_packet(service, version, node)
                    self._repository.remove_pending_instance(service, version, node)
                    self.logger.info('%s activated', (service, version))
                else:
                    self.logger.info('%s can\'t register because it depends on %s', (service, version), vendor)

    def _make_activated_packet(self, service, version):
        vendors = self._repository.get_vendors(service, version)
        instances = {
            (v['service'], v['version']): self._repository.get_versioned_instances(v['service'], v['version'])
            for v in vendors}
        return ControlPacket.activated(instances)

    def _connect_to_service(self, host, port, node_id, service_type):
        if service_type == 'tcp':
            if node_id not in self._service_protocols:
                coroutine = self._loop.create_connection(partial(get_vyked_protocol, self), host, port)
                future = asyncio.async(coroutine)
                future.add_done_callback(partial(self._handle_service_connection, node_id, host, port))
        elif service_type == 'http':
            if not (host, port) in self._http_pingers:
                pinger = HTTPPinger(host, port, node_id, self)
                self._http_pingers[(host, port)] = pinger
                pinger.ping()

    def _handle_service_connection(self, node_id, host, port, future):
        transport, protocol = future.result()
        self._service_protocols[node_id] = protocol
        pinger = TCPPinger(host, port, node_id, protocol, self)
        self._tcp_pingers[node_id] = pinger
        pinger.ping()

    def _notify_consumers(self, service, version, node_id):
        packet = ControlPacket.deregister(service, version, node_id)
        for consumer_name, consumer_version in self._repository.get_consumers(service, version):
            for host, port, node, service_type in self._repository.get_instances(consumer_name, consumer_version):
                protocol = self._client_protocols[node]
                protocol.send(packet)

    def get_service_instances(self, packet, registry_protocol):
        params = packet['params']
        service, version = params['service'].lower(), params['version']
        instances = self._repository.get_instances(service, version)
        instance_packet = ControlPacket.send_instances(service, version, packet['request_id'], instances)
        registry_protocol.send(instance_packet)

    def get_subscribers(self, packet, protocol):
        params = packet['params']
        request_id = packet['request_id']
        service, version, endpoint = params['service'].lower(), params['version'], params['endpoint']
        subscribers = self._repository.get_subscribers(service, version, endpoint)
        packet = ControlPacket.subscribers(service, version, endpoint, request_id, subscribers)
        protocol.send(packet)

    def on_timeout(self, host, port, node_id):
        service = self._repository.get_node(node_id)
        self.logger.debug('%s timed out', service)
        self.deregister_service(host, port, node_id)

    def _ping(self, packet):
        pinger = self._tcp_pingers[packet['node_id']]
        pinger.pong_received()

    def _pong(self, packet, protocol):
        protocol.send(ControlPacket.pong(packet['node_id']))

    def _xsubscribe(self, packet):
        params = packet['params']
        service, version, host, port, node_id = (params['service'], params['version'], params['host'], params['port'],
                                                 params['node_id'])
        endpoints = params['events']
        self._repository.xsubscribe(service, version, host, port, node_id, endpoints)

    def _get_uptime_report(self, packet, protocol):
        uptimes = self._repository.get_uptimes()
        protocol.send(ControlPacket.uptime(uptimes))

    def periodic_uptime_logger(self):
        self._repository.log_uptimes()
        asyncio.get_event_loop().call_later(300, self.periodic_uptime_logger)


if __name__ == '__main__':
    # config_logs(enable_ping_logs=False, log_level=logging.DEBUG)
    from setproctitle import setproctitle

    setproctitle("registry")
    REGISTRY_HOST = None
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT, Repository())
    registry.periodic_uptime_logger()
    registry.start()
