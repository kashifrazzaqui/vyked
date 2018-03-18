import logging
import signal
import asyncio
from functools import partial
from collections import defaultdict, namedtuple
import time
import json
import ssl

from again.utils import natural_sort

from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol
from .pinger import TCPPinger, HTTPPinger
from .utils.log import setup_logging

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
        self._uptimes[service_name][service.host][service.port] = {
            'uptime': int(time.time()),
            'node_id': service.node_id
        }

        instance_name = self._get_full_instance_name(service.name, service.version, service.node_id)
        if len(service.dependencies):
            if self._service_dependencies.get(instance_name) is None:
                self._service_dependencies[instance_name] = service.dependencies

    def is_pending(self, service, version):
        return self._get_full_service_name(service, version) in self._pending_services

    def add_pending_service(self, service, version, node_id):
        full_service_name = self._get_full_service_name(service, version)
        if node_id not in self._pending_services[full_service_name]:
            self._pending_services[full_service_name].append(node_id)

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

    def get_vendors(self, service_name, version, node_id):
        return self._service_dependencies.get(self._get_full_instance_name(service_name, version, node_id), [])

    def get_node(self, node_id):
        for name, versions in self._registered_services.items():
            for version, instances in versions.items():
                for host, port, node, service_type in instances:
                    if node_id == node:
                        return Service(name, version, [], host, port, node, service_type)
        return None

    def remove_node(self, node_id):
        thehost = None
        theport = None
        for name, versions in self._registered_services.items():
            for version, instances in versions.items():
                to_remove = []
                for instance in instances:
                    host, port, node, service_type = instance
                    if node_id == node:
                        thehost = host
                        theport = port
                        to_remove.append(instance)
                        try:
                            self.remove_pending_instance(name, version, node_id)
                        except ValueError:
                            pass
                for instance in to_remove:
                    instances.remove(instance)
        remove_from_uptimes = []
        for name, nodes in self._uptimes.items():
            for host, portup in nodes.items():
                for port, uptimes in portup.items():
                    if host == thehost and port == theport and uptimes['node_id'] == node_id:
                        uptimes['downtime'] = int(time.time())
                        remove_from_uptimes.append((name, host, port))
        for name, host, port in remove_from_uptimes:
            self._uptimes[name][host].pop(port)
        self.log_uptimes()
        return None

    def get_uptimes(self):
        return self._uptimes

    def log_uptimes(self):
        for name, nodes in self._uptimes.items():
            for host, portup in nodes.items():
                for port, d in portup.items():
                    now = int(time.time())
                    live = d.get('downtime', 0) < d['uptime']
                    uptime = now - d['uptime'] if live else 0
                    logd = {'service_name': name.split('/')[0], 'hostname': host, 'status': live,
                            'uptime': int(uptime)}
                    logging.getLogger('stats').info(logd)

    def xsubscribe(self, service, version, host, port, node_id, endpoints):
        entry = (service, version)
        # Remove all entries of service, version from subscribe list
        for name, versions in self._subscribe_list.items():
            for p_version, endpoints2 in versions.items():
                for endpoint, subscribers in endpoints2.items():
                    to_remove = list(filter(lambda x: service == x[0] and version == x[1], 
                        subscribers))
                    for subscriber in to_remove:
                        subscribers.remove(subscriber)

        # Add entries of service, version into subscribe list - thus keeping 
        # only the latest information
        for endpoint in endpoints:
            self._subscribe_list[endpoint['service']][endpoint['version']][endpoint['endpoint']].append(
                entry)

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
    def _get_full_instance_name(service: str, version, node_id):
        return '{}/{}/{}'.format(service, version, node_id)

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
        self._blacklisted_hosts = {}
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
            self.register_service(packet, protocol, transport)
        elif request_type == 'get_instances':
            self.get_service_instances(packet, protocol)
        elif request_type == 'xsubscribe':
            self._xsubscribe(packet)
        elif request_type == 'get_subscribers':
            self.get_subscribers(packet, protocol)
        elif request_type == 'pong':
            self._ping(packet)
        elif request_type == 'ping':
            self._handle_ping(packet, protocol, transport)
        elif request_type == 'uptime_report':
            self._get_uptime_report(protocol)
        elif request_type == 'change_log_level':
            self._handle_log_change(packet, protocol)
        # API for graceful_shutdown
        elif request_type == 'blacklist_service':
            self._handle_blacklist(packet, protocol, transport)
        elif request_type == 'whitelist_service':
            self._handle_whitelist(packet, protocol)
        elif request_type == 'show_blacklisted':
            self._show_blacklisted(protocol)
        elif request_type == 'show_current_state':
            self._show_current_state(protocol)

    def deregister_service(self, host, port, node_id):
        service = self._repository.get_node(node_id)
        self._tcp_pingers.pop(node_id, None)
        self._http_pingers.pop(node_id, None)
        if service:
            for_log = {"caller_name": service.name + '/' + service.version, "caller_address": service.host,
                       "request_type": 'deregister'}
            self.logger.debug(for_log)
            self._repository.remove_node(node_id)
            instance_name = self._repository._get_full_instance_name(service.name, service.version, node_id)
            self._repository._service_dependencies.pop(instance_name, None)
            if service is not None:
                self._service_protocols.pop(node_id, None)
                self._client_protocols.pop(node_id, None)
                self._notify_consumers(service.name, service.version, node_id)
                if not len(self._repository.get_instances(service.name, service.version)):
                    consumers = self._repository.get_consumers(service.name, service.version)
                    for consumer_name, consumer_version, _ in consumers:
                        for _, _, node_id, _ in self._repository.get_instances(consumer_name, consumer_version):
                            self._repository.add_pending_service(consumer_name, consumer_version, node_id)

    def register_service(self, packet: dict, registry_protocol, transport=None):
        params = packet['params']
        if params['host'] == '0.0.0.0' and transport:
            params['host'] = transport.get_extra_info("peername")[0]
        service = Service(params['service'], params['version'], params['dependencies'], params['host'], params['port'],
                          params['node_id'], params['type'])
        self._repository.register_service(service)
        self._client_protocols[params['node_id']] = registry_protocol
        self._connect_to_service(params['host'], params['port'], params['node_id'], params['type'])
        self._handle_pending_registrations()
        self._inform_consumers(service)

    def _inform_consumers(self, service: Service):
        consumers = self._repository.get_consumers(service.name, service.version)
        for service_name, service_version, node in consumers:
            # if not self._repository.is_pending(service_name, service_version):
                # instances = self._repository.get_instances(service_name, service_version)
                # for host, port, node, stype in instances:
            protocol = self._client_protocols[node]
            protocol.send(ControlPacket.new_instance(
                service.name, service.version, service.host, service.port, service.node_id, service.type))

    def _send_activated_packet(self, service, version, node):
        protocol = self._client_protocols.get(node, None)
        if protocol:
            packet = self._make_activated_packet(service, version, node)
            protocol.send(packet)

    def _handle_pending_registrations(self):
        for service, version in self._repository.get_pending_services():
            pending_nodes = list(self._repository.get_pending_instances(service, version))
            for node in pending_nodes:
                vendors = self._repository.get_vendors(service, version, node)
                should_activate = True
                for vendor in vendors:
                    instances = self._repository.get_versioned_instances(vendor['service'], vendor['version'])
                    tcp_instances = [instance for instance in instances if instance[3] == 'tcp']
                    if not len(tcp_instances):
                        should_activate = False
                        break
                self._send_activated_packet(service, version, node)
                if should_activate:
                    self._repository.remove_pending_instance(service, version, node)
                    self.logger.info('%s activated', (service, version))
                else:
                    self.logger.info('%s can\'t register because it depends on %s', (service, version), vendor)

    def _make_activated_packet(self, service, version, node):
        vendors = self._repository.get_vendors(service, version, node)
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
            pass
            if not (host, port) in self._http_pingers:
               pinger = HTTPPinger(host, port, node_id, self)
               self._http_pingers[node_id] = pinger
               pinger.ping(node_id)

    def _handle_service_connection(self, node_id, host, port, future):
        transport, protocol = future.result()
        self._service_protocols[node_id] = protocol
        pinger = TCPPinger(host, port, node_id, protocol, self)
        if node_id in self._tcp_pingers:
            self._tcp_pingers[node_id].stop()
        self._tcp_pingers[node_id] = pinger
        pinger.ping()

    def _notify_consumers(self, service, version, node_id):
        packet = ControlPacket.deregister(service, version, node_id)
        for consumer_name, consumer_version, _ in self._repository.get_consumers(service, version):
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
        node_id = packet['node_id']
        if node_id in self._tcp_pingers:
            pinger = self._tcp_pingers[node_id]
            pinger.pong_received()

    def _pong(self, packet, protocol):
        protocol.send(ControlPacket.pong(packet['node_id']))

    def _xsubscribe(self, packet):
        params = packet['params']
        service, version, host, port, node_id = (params['service'], params['version'], params['host'], params['port'],
                                                 params['node_id'])
        endpoints = params['events']
        self._repository.xsubscribe(service, version, host, port, node_id, endpoints)

    def _get_uptime_report(self, protocol):
        uptimes = self._repository.get_uptimes()
        protocol.send(ControlPacket.uptime(uptimes))

    def periodic_uptime_logger(self):
        self._repository.log_uptimes()
        asyncio.get_event_loop().call_later(300, self.periodic_uptime_logger)

    def _handle_log_change(self, packet, protocol):
        try:
            level = getattr(logging, packet['level'].upper())
        except KeyError as e:
            self.logger.error(e)
            protocol.send('Malformed packet')
            return
        except AttributeError as e:
            self.logger.error(e)
            protocol.send('Allowed logging levels: DEBUG, INFO, WARNING, ERROR, CRITICAL')
            return
        logging.getLogger().setLevel(level)
        for handler in logging.getLogger().handlers:
            handler.setLevel(level)
        protocol.send('Logging level updated')

    def _handle_ping(self, packet, protocol, transport):
        """ Responds to pings from registry_client only if the node_ids present in the ping payload are registered
        :param packet: The 'ping' packet received
        :param protocol: The protocol on which the pong should be sent
        """
        payload = packet.get('payload', {}).values()
        if all(map(self._repository.get_node, payload)) or not payload or \
              (transport.get_extra_info("peername")[0]) in self._blacklisted_hosts:
            self._pong(packet, protocol)

    def _handle_blacklist(self, packet, protocol, transport):
        host_ip = packet['ip']
        host_port = packet.get('port', 0)
        if host_ip == '0.0.0.0' and transport:
            host_ip = transport.get_extra_info("peername")[0]
        """ If port is 0 then deregister all services on given IP """
        if host_ip not in self._blacklisted_hosts:
            self._blacklisted_hosts[host_ip] = []
        deregister_list = []
        count = 0
        for name, versions in self._repository._registered_services.items():
            for version, instances in versions.items():
                for host, port, node, service_type in instances:
                    if (not host_port and host_ip == host) or (host_port and host_port == port and host_ip == host):
                        deregister_list.append([host, port, node])
                        if port not in self._blacklisted_hosts[host_ip]:
                            self._blacklisted_hosts[host_ip].append(port)
                            count += 1
        if not count:
            protocol.send("No Sevice currently running on " + str(host_ip) + ":" + str(host_port))
            if not len(self._blacklisted_hosts[host_ip]):
                self._blacklisted_hosts.pop(host_ip, None)
            return

        for host, port, node in deregister_list:
            self.deregister_service(host, port, node)

        """ For subscription can either  Remove service from subscribed list or can only let
            registered services subscribe """

        protocol.send("Blacklisted Services on " + str(host_ip) + ":" + str(host_port))

    def _handle_whitelist(self, packet, protocol):
        wtlist_ip = packet['ip']
        wtlist_port = packet.get('port', 0)
        if wtlist_ip not in self._blacklisted_hosts:
            protocol.send(str(wtlist_ip) + " currently not in blacklist ")
            return
        else:
            if (wtlist_port not in self._blacklisted_hosts[wtlist_ip]) and wtlist_port:
                protocol.send(str(wtlist_ip) + ":" + str(wtlist_port) + " currently not in blacklist ")
                return
            elif wtlist_port in self._blacklisted_hosts[wtlist_ip]:
                self._blacklisted_hosts[wtlist_ip].remove(wtlist_port)
                if not self._blacklisted_hosts[wtlist_ip]:
                    self._blacklisted_hosts.pop(wtlist_ip, None)
                protocol.send("Whitelisted Services on " + str(wtlist_ip) + ":" + str(wtlist_port))
            else:
                self._blacklisted_hosts.pop(wtlist_ip, None)
                protocol.send("Whitelisted Services on " + str(wtlist_ip))

    def _show_blacklisted(self, protocol):
        data = self._blacklisted_hosts
        protocol.send(str(data))

    def _show_current_state(self, protocol):
        status_dict = {'Registered Services': self._repository._registered_services,
                       'Pending Services': self._repository._pending_services,
                       'XSubscription List': self._repository._subscribe_list,
                       'Service Dependencies': self._repository._service_dependencies}
        protocol.send(status_dict)

if __name__ == '__main__':
    # config_logs(enable_ping_logs=False, log_level=logging.DEBUG)
    from setproctitle import setproctitle

    setproctitle("registry")
    REGISTRY_HOST = None
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT, Repository())
    registry.periodic_uptime_logger()
    registry.start()
