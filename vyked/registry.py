import logging
import signal
import asyncio
from functools import partial
from collections import defaultdict, namedtuple
from again.utils import natural_sort
import time

from .utils.log import config_logs
from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol
from .pinger import TCPPinger, HTTPPinger
from .utils.log import setup_logging

import json
from cauldron import PostgresStore
import ssl

Service = namedtuple('Service', ['name', 'version', 'dependencies', 'host', 'port', 'node_id', 'type'])
logger = logging.getLogger('vyked.registry')


def tree():
    return defaultdict(tree)


def json_file_to_dict(_file: str) -> dict:
    with open(_file) as config_file:
        config = json.load(config_file)
    return config

sno = 0

# def p(func):
#     def wrapper(*a, **b):
#         global sno
#         num = sno
#         sno = sno+1
#         a = yield from func(*a, **b)
#         print("Sno:"+str(num))
#         print("Function:"+str(func.__name__))
#         print("Value:"+str(a))
#         return a
#     return wrapper

class Repository:

    def __init__(self):
        self._registered_services = defaultdict(lambda: defaultdict(list))
        self._pending_services = defaultdict(list)
        self._service_dependencies = {}
        self._subscribe_list = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self._uptimes = tree()

    def register_service(self, service: Service):
        service_name = self._get_full_service_name(service.name, service.version)
        service_entry = (service.host, service.port, service.node_id, service.type)
        self._registered_services[service.name][service.version].append(service_entry)
        self._pending_services[service_name].append(service.node_id)
        self._uptimes[service_name][service.host]['uptime'] = int(time.time())

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
                    thehost, port, node, service_type = instance
                    if node_id == node:
                        instances.remove(instance)
        for name, nodes in self._uptimes.items():
            for host, uptimes in nodes.items():
                if host == thehost:
                    uptimes['downtime'] = int(time.time())
        return None

    def get_uptimes(self):
        return self._uptimes

    def log_uptimes(self):
        for name, nodes in self._uptimes.items():
            for host, d in nodes.items():
                now = time.time()
                live = d.get('downtime', 0) < d['uptime']
                uptime = now - d['uptime'] if live else 0
                logd = {'service': name, 'host': host, 'status': live,
                        'uptime': int(uptime)}
                logger.info(logd)

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


class PersistentRepository(PostgresStore):
    def __init__(self):
        try:
            config = json_file_to_dict('./config.json')
            self.connect(database=config['POSTGRES_DB'], user=config['POSTGRES_USER'],
                                         password=config['POSTGRES_PASS'], host=config['POSTGRES_HOST'],
                                         port=config['POSTGRES_PORT'])
        except Exception as e:
            logger.error(str(e))

    def register_service(self, service: Service):
        service_dict = {'service_name': service.name, 'version': service.version, 'ip': service.host,
                       'port': service.port, 'protocol': service.type, 'node_id': service.node_id, 'is_pending': True}

        yield from self.insert('services', service_dict)
        uptimes_dict = {'node_id': service.node_id, 'event_type': 'UPTIME', 'event_time': int(time.time())}
        yield from self.insert('uptimes', uptimes_dict)
        if len(service.dependencies):
            for parent in service.dependencies:
                dependencies_dict = {'child_name': service.name, 'child_version': service.version,
                                     'parent_name': parent['service'], 'parent_version': parent['version']}
                yield from self.insert('dependencies', dependencies_dict)

    def is_pending(self, service, version):
        rows = yield from self.select('services', 'service_name', columns=['service_name', 'version'],
                            where_keys=[{'service_name': ('=', service), 'version': ('=', version),
                                         'is_pending': ('=', True)}])
        if len(rows):
            return True
        else:
            return False

    def add_pending_service(self, service, version, node_id):
        yield from self.update('services', values={'is_pending': True}, where_keys=[{'node_id': ('=', node_id)}])

    def get_pending_services(self):
        rows = yield from self.select('services', 'service_name', columns=['service_name', 'version'],
                            where_keys=[{'is_pending': ('=', True)}])
        return rows

    def get_pending_instances(self, service, version):
        rows = yield from self.select('services', 'node_id', columns=['node_id'], where_keys=[{'is_pending': ('=', True),
                                                                                     'service_name': ('=', service),
                                                                                     'version': ('=', version)}])
        crows = [x for (x,) in rows]
        return crows

    def remove_pending_instance(self, service, version, node_id):
        yield from self.update('services', values={'is_pending': False}, where_keys=[{'node_id': ('=', node_id)}])

    def get_instances(self, service, version):
        rows = yield from self.select('services', 'port', columns=['ip', 'port', 'node_id', 'protocol'],
                            where_keys=[{'service_name': ('=', service), 'version': ('=', version)}])
        return rows

    def get_versioned_instances(self, service, version):
        rows = yield from self.select('services', 'version', columns=['version'], where_keys=[{'service_name': ('=', service)}])
        crows = [x for (x,) in rows]
        version = self._get_non_breaking_version(version, crows)
        rows2 = yield from self.select('services', 'port', columns=['ip', 'port', 'node_id', 'protocol'],
                                       where_keys=[{'service_name': ('=', service), 'version': ('=', version)}])
        return rows2

    def get_consumers(self, service_name, service_version):
        rows = yield from self.select('dependencies', 'child_name',columns=['child_name', 'child_version'],
                            where_keys=[{'parent_name': ('=', service_name), 'parent_version': ('=', service_version)}])
        crows = set(rows)
        return crows

    def get_vendors(self, service, version):
        rows = yield from self.select('dependencies', 'parent_name',columns=['parent_name', 'parent_version'],
                            where_keys=[{'child_name': ('=', service), 'child_version': ('=', version)}])
        crows = [{'service': s, 'version': v} for (s,v) in rows]
        return crows

    def get_node(self, node_id):
        rows = yield from self.select('services', 'service_name',
                            columns=['service_name','version', 'ip', 'port', 'node_id', 'protocol'],
                            where_keys=[{'node_id': ('=', node_id)}])
        for name, version, host, port, node, protocol in rows:
            return Service(name, version, [], host, port, node, protocol)
        return None

    def remove_node(self, node_id):
        yield from self.delete('services', where_keys=[{'node_id': ('=', node_id)}])
        uptimes_dict = {'node_id': node_id, 'event_type': 'DOWNTIME', 'event_time': int(time.time())}
        yield from self.insert('uptimes', uptimes_dict)
        return None

    def get_uptimes(self):
        rows = yield from self.select('uptimes', 'node_id', columns=['node_id', 'event_type', 'event_time'])
        return rows

    # def log_uptimes(self):
    #     for name, nodes in self._uptimes.items():
    #         for host, d in nodes.items():
    #             now = time.time()
    #             live = d.get('downtime', 0) < d['uptime']
    #             uptime = now - d['uptime'] if live else 0
    #             logd = {'service': name, 'host': host, 'status': live,
    #                     'uptime': int(uptime)}
    #             logger.info(logd)


    def xsubscribe(self, service, version, host, port, node_id, endpoints):
        for endpoint in endpoints:
            subscription_dict = {'subscriber_name': service, 'subscriber_version': version,
                                 'subscribee_name': endpoint['service'], 'subscribee_version': endpoint['version'],
                                 'event_name': endpoint['endpoint'], 'strategy': endpoint['strategy']}
            yield from self.insert('subscriptions', subscription_dict)

    def get_subscribers(self, service, version, endpoint):
        query = "select subscriber_name, subscriber_version, ip, port, node_id, strategy " \
                "from services as sv, subscriptions as sb where sb.subscribee_name = %s and " \
                "sb.subscribee_version = %s and sb.event_name = %s and sb.subscriber_name = sv.service_name and " \
                "sb.subscriber_version = sv.version"
        rows = yield from self.raw_sql(query, (service, version, endpoint))
        return rows

    def deactivate_dependencies(self, service, version):
        query = "update services set is_pending = True from dependencies where service_name = child_name and " \
                "version = child_version and parent_name = %s and parent_version = %s"
        yield from self.raw_sql(query, (service,version))

    def inform_consumers(self, service, version):
        query = "select host, port, node_id, protocol from services, dependencies where is_pending='False' and "\
                "service_name = child_name and verion = child_version and parent_name = %s and parent_version = %s"
        rows = yield from self.raw_sql(query, (service, version))
        return rows

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


    def get_registered_services(self):
        rows = yield from self.select('services', 'port', columns=['ip', 'port', 'node_id', 'protocol', 'service_name',
                                                                   'version'])
        return rows

class Registry:

    def __init__(self, ip, port, repository: PersistentRepository):
        self._ip = ip
        self._port = port
        self._loop = asyncio.get_event_loop()
        self._repository = repository
        self._client_protocols = {}
        self._service_protocols = {}
        self._pingers = {}
        self.logger = logging.getLogger()
        try:
            config = json_file_to_dict('./config.json')
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self._ssl_context.load_cert_chain(config['SSL_CERTIFICATE'], config['SSL_KEY'])
        except:
            self._ssl_context = None
        # self._ssl_context = None

    def start(self):
        setup_logging("registry")
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))
        registry_coroutine = self._loop.create_server(
            partial(get_vyked_protocol, self), self._ip, self._port, ssl=self._ssl_context)
        server = self._loop.run_until_complete(registry_coroutine)
        self._loop.run_until_complete(self.reconnect_on_recovery())
        try:
            self._loop.run_forever()
        except Exception as e:
            self.logger.error(e)
        finally:
            server.close()
            self._loop.run_until_complete(server.wait_closed())
            self._loop.close()

    @asyncio.coroutine
    def reconnect_on_recovery(self):
        for (host, port, node, type, _, _) in (yield from self._repository.get_registered_services()):
            try:
                self._connect_to_service(host, port, node, type)
            except Exception as e:
                logger.error(str(e))


    def _stop(self, signame: str):
        print('\ngot signal {} - exiting'.format(signame))
        self._loop.stop()

    def receive(self, packet: dict, protocol, transport):
        request_type = packet['type']
        if request_type == 'register':
            asyncio.async(self.register_service(packet, protocol))
        elif request_type == 'get_instances':
            asyncio.async(self.get_service_instances(packet, protocol))
        elif request_type == 'xsubscribe':
            asyncio.async(self._xsubscribe(packet))
        elif request_type == 'get_subscribers':
            asyncio.async(self.get_subscribers(packet, protocol))
        elif request_type == 'pong':
            self._ping(packet)
        elif request_type == 'ping':
            try:
                self._client_protocols[packet['node_id']] = protocol
            except Exception as e:
                logger.error(str(e))
            self._pong(packet, protocol)
        elif request_type == 'uptime_report':
            asyncio.async(self._get_uptime_report(packet, protocol))

    @asyncio.coroutine
    def deregister_service(self, node_id):
        service = yield from self._repository.get_node(node_id)
        yield from self._repository.remove_node(node_id)
        if service is not None:
            self._service_protocols.pop(node_id, None)
            self._client_protocols.pop(node_id, None)
            asyncio.async(self._notify_consumers(service.name, service.version, node_id))
            if not len((yield from self._repository.get_instances(service.name, service.version))):
                #TODO: Call self._repository.deactivate_dependencies(service.name, service.version) instead of multiple queries
                consumers = yield from self._repository.get_consumers(service.name, service.version)
                for consumer_name, consumer_version in consumers:
                    for _, _, node_id, _ in (yield from self._repository.get_instances(consumer_name, consumer_version)):
                        yield from self._repository.add_pending_service(consumer_name, consumer_version, node_id)

    @asyncio.coroutine
    def register_service(self, packet: dict, registry_protocol):
        params = packet['params']
        service = Service(params['service'], params['version'], params['dependencies'], params['host'], params['port'],
                          params['node_id'], params['type'])
        yield from self._repository.register_service(service)
        self._client_protocols[params['node_id']] = registry_protocol
        if params['node_id'] not in self._service_protocols.keys():
            self._connect_to_service(params['host'], params['port'], params['node_id'], params['type'])
        asyncio.async(self._handle_pending_registrations())
        asyncio.async(self._inform_consumers(service))

    @asyncio.coroutine
    def _inform_consumers(self, service: Service):
        #TODO: Call self._repository.inform_consumers(service.name, service.version) instead of multiple queries
        consumers = yield from self._repository.get_consumers(service.name, service.version)
        for service_name, service_version in consumers:
            if not (yield from self._repository.is_pending(service_name, service_version)):
                instances = self._repository.get_instances(service_name, service_version)
                for host, port, node, type in instances:
                    protocol = self._client_protocols[node]
                    protocol.send(ControlPacket.new_instance(
                        service.name, service.version, service.host, service.port, service.node_id, service.type))

    @asyncio.coroutine
    def _send_activated_packet(self, service, version, node):
        protocol = self._client_protocols.get(node, None)
        if protocol:
            packet = yield from (self._make_activated_packet(service, version))
            protocol.send(packet)

    @asyncio.coroutine
    def _handle_pending_registrations(self):
        for service, version in (yield from self._repository.get_pending_services()):
            vendors = yield from self._repository.get_vendors(service, version)
            should_activate = True
            for vendor in vendors:
                instances = yield from self._repository.get_versioned_instances(vendor['service'], vendor['version'])
                tcp_instances = [instance for instance in instances if instance[3] == 'tcp']
                if not len(tcp_instances):
                    should_activate = False
                    break
            for node in (yield from self._repository.get_pending_instances(service, version)):
                if should_activate:
                    asyncio.async(self._send_activated_packet(service, version, node))
                    yield from self._repository.remove_pending_instance(service, version, node)
                    logger.info('%s activated', (service, version))
                else:
                    logger.info('%s can\'t register because it depends on %s', (service, version), vendor)

    @asyncio.coroutine
    def _make_activated_packet(self, service, version):
        vendors = yield from self._repository.get_vendors(service, version)
        instances = {}
        for v in vendors:
            ins = yield from self._repository.get_versioned_instances(v['service'], v['version'])
            instances[(v['service'], v['version'])] = ins
        return ControlPacket.activated(instances)

    def _connect_to_service(self, host, port, node_id, service_type):
        if service_type == 'tcp':
            coroutine = self._loop.create_connection(partial(get_vyked_protocol, self), host, port)
            future = asyncio.async(coroutine)
            future.add_done_callback(partial(self._handle_service_connection, node_id))
        elif service_type == 'http':
            pinger = HTTPPinger(node_id, host, port, self)
            self._pingers[node_id] = pinger
            pinger.ping()

    def _handle_service_connection(self, node_id, future):
        transport, protocol = future.result()
        self._service_protocols[node_id] = protocol
        pinger = TCPPinger(node_id, protocol, self)
        self._pingers[node_id] = pinger
        pinger.ping()

    @asyncio.coroutine
    def _notify_consumers(self, service, version, node_id):
        packet = ControlPacket.deregister(service, version, node_id)
        for consumer_name, consumer_version in (yield from self._repository.get_consumers(service, version)):
            for host, port, node, service_type in (yield from self._repository.get_instances(consumer_name, consumer_version)):
                protocol = self._client_protocols[node]
                protocol.send(packet)

    @asyncio.coroutine
    def get_service_instances(self, packet, registry_protocol):
        params = packet['params']
        service, version = params['service'].lower(), params['version']
        instances = yield from self._repository.get_instances(service, version)
        instance_packet = ControlPacket.send_instances(service, version, packet['request_id'], instances)
        registry_protocol.send(instance_packet)

    @asyncio.coroutine
    def get_subscribers(self, packet, protocol):
        params = packet['params']
        request_id = packet['request_id']
        service, version, endpoint = params['service'].lower(), params['version'], params['endpoint']
        subscribers = yield from self._repository.get_subscribers(service, version, endpoint)
        packet = ControlPacket.subscribers(service, version, endpoint, request_id, subscribers)
        protocol.send(packet)

    def on_timeout(self, node_id):
        asyncio.async(self.deregister_service(node_id))

    def _ping(self, packet):
        pinger = self._pingers[packet['node_id']]
        pinger.pong_received()

    def _pong(self, packet, protocol):
        protocol.send(ControlPacket.pong(packet['node_id']))

    @asyncio.coroutine
    def _xsubscribe(self, packet):
        params = packet['params']
        service, version, host, port, node_id = (params['service'], params['version'], params['host'], params['port'],
                                                 params['node_id'])
        endpoints = params['events']
        yield from self._repository.xsubscribe(service, version, host, port, node_id, endpoints)

    @asyncio.coroutine
    def _get_uptime_report(self, packet, protocol):
        uptimes = yield from self._repository.get_uptimes()
        dict_uptimes = {}
        for u in uptimes:
            dict_uptimes[u.node_id][u.event_type] = u.event_time
        protocol.send(ControlPacket.uptime(dict_uptimes))

    # def periodic_uptime_logger(self):
    #     self._repository.log_uptimes()
    #     asyncio.get_event_loop().call_later(300, self.periodic_uptime_logger)


if __name__ == '__main__':
    config_logs(enable_ping_logs=False, log_level=logging.DEBUG)
    from setproctitle import setproctitle

    setproctitle("registry")
    REGISTRY_HOST = None
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT, PersistentRepository())
    # registry.periodic_uptime_logger()
    registry.start()

