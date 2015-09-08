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
import psycopg2
from cauldron import PostgresStore
import sys

Service = namedtuple('Service', ['name', 'version', 'dependencies', 'host', 'port', 'node_id', 'type'])
logger = logging.getLogger(__name__)

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

    def register_service(self, service: Service):
        service_name = self._get_full_service_name(service.name, service.version)
        service_entry = (service.host, service.port, service.node_id, service.type)
        self._registered_services[service.name][service.version].append(service_entry)
        self._pending_services[service_name].append(service.node_id)
        self._uptimes[service_name][service.node_id]['uptime'] = int(time.time())
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
        for name, versions in self._registered_services.items():
            for version, instances in versions.items():
                for instance in instances:
                    host, port, node, service_type = instance
                    if node_id == node:
                        instances.remove(instance)
        for name, nodes in self._uptimes.items():
            for node, uptimes in nodes.items():
                if node == node_id:
                    uptimes['downtime'] = int(time.time())
        return None

    def get_uptimes(self):
        return self._uptimes

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

        config = json_file_to_dict('./config.json')
        self.conn = psycopg2.connect(database=config['POSTGRES_DB'], user=config['POSTGRES_USER'],
                                     password=config['POSTGRES_PASS'], host=config['POSTGRES_HOST'],
                                     port=config['POSTGRES_PORT'])
        self.cur = self.conn.cursor()
        # self._registered_services = defaultdict(lambda: defaultdict(list))
        # self._pending_services = defaultdict(list)
        # self._service_dependencies = {}
        # self._subscribe_list = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        # self._uptimes = tree()

    def qinsert(self, table: str, values: dict):
        keys = self._COMMA.join(values.keys())
        value_place_holder = self._PLACEHOLDER * len(values)
        query = self._insert_string.format(table, keys, value_place_holder[:-1])
        print(query % tuple(values.values()))
        try:
            self.cur.execute(query, tuple(values.values()))
        except:
            print(sys.exc_info()[0])
        self.conn.commit()

    def qselect(self, table: str, order_by: str, columns: list=None, where_keys: list=None, limit=100,
               offset=0):
        if columns:
            columns_string = ', '.join(columns)
            if where_keys:
                where_clause, values = self._get_where_clause_with_values(where_keys)
                query = self._select_selective_column_with_condition.format(columns_string, table, where_clause,
                                                                           order_by, limit, offset)
                q, t = query, values
            else:
                query = self._select_selective_column.format(columns_string, table, order_by, limit, offset)
                q, t = query, ()
        else:
            if where_keys:
                where_clause, values = self._get_where_clause_with_values(where_keys)
                query = self._select_all_string_with_condition.format(table, where_clause, order_by, limit, offset)
                q, t = query, values
            else:
                query = self._select_all_string.format(table, order_by, limit, offset)
                q, t = query, ()
        print(q % t)
        try:
            self.cur.execute(q, t)
        except:
            print(sys.exc_info()[0])

        return self.cur.fetchall()

    def qdelete(self, table: str, where_keys: list):
        where_clause, values = self._get_where_clause_with_values(where_keys)
        query = self._delete_query.format(table, where_clause)
        print(query % values)
        try:
            self.cur.execute(query, values)
        except:
            print(sys.exc_info()[0])
        self.conn.commit()

    def qupdate(self, table: str, values: dict, where_keys: list):
        keys = self._COMMA.join(values.keys())
        value_place_holder = self._PLACEHOLDER * len(values)
        where_clause, where_values = self._get_where_clause_with_values(where_keys)
        query = self._update_string.format(table, keys, value_place_holder[:-1], where_clause)
        print(query % (tuple(values.values()) + where_values))
        try:
            self.cur.execute(query, (tuple(values.values()) + where_values))
        except:
            print(sys.exc_info()[0])
        self.conn.commit()

    def register_service(self, service: Service):
        service_name = self._get_full_service_name(service.name, service.version)
        service_entry = (service.host, service.port, service.node_id, service.type)
        #TODO: Insert new Service, ispending=true, add into uptimes also

        service_dict = {'service_name': service.name, 'version': service.version, 'ip': service.host,
                       'port': service.port, 'protocol': service.type, 'id': service.node_id, 'is_pending': True}

        self.qinsert('services', service_dict)
        uptimes_dict = {'id': service.node_id, 'event_type': 'UPTIME', 'event_time': int(time.time())}
        self.qinsert('uptimes', uptimes_dict)

        # self._registered_services[service.name][service.version].append(service_entry)
        # self._pending_services[service_name].append(service.node_id)
        # self._uptimes[service_name][service.node_id]['uptime'] = int(time.time())
        if len(service.dependencies):
            #TODO: Insert into dependencies
            for parent in service.dependencies:
                dependencies_dict = {'child_name': service.name, 'child_version': service.version,
                                     'parent_name': parent['service'], 'parent_version': parent['version']}
                self.qinsert('dependencies', dependencies_dict)
            # if self._service_dependencies.get(service_name) is None:
            #     self._service_dependencies[service_name] = service.dependencies

    def is_pending(self, service, version):
        rows = self.qselect('services', 'service_name', columns=['service_name', 'version'],
                            where_keys=[{'service_name': ('=', service), 'version': ('=', version),
                                         'is_pending': ('=', True)}])
        if len(rows):
            return True
        else:
            return False
        # return self._get_full_service_name(service, version) in self._pending_services


    def add_pending_service(self, service, version, node_id):
        #TODO: update the is_pending flag in the record
        self.qupdate('services', values={'is_pending': True}, where_keys=[{'id': ('=', node_id)}])
        # self._pending_services[self._get_full_service_name(service, version)].append(node_id)

    def get_pending_services(self):
        #TODO: select * from services where is_pending flag is true
        rows = self.qselect('services', 'service_name', columns=['service_name', 'version'], where_keys=[{'is_pending': ('=', True)}])
        print("get_pending_services")
        print(rows)
        return rows
        # print([self._split_key(k) for k in self._pending_services.keys()])
        # return [self._split_key(k) for k in self._pending_services.keys()]

    def get_pending_instances(self, service, version):
        #TODO: select * from services where is_pending flag is true
        rows = self.qselect('services', 'id',columns=['id'], where_keys=[{'is_pending': ('=', True),
                                                                               'service_name': ('=', service),
                                                                               'version': ('=', version)}])
        print("get_pending_instances")
        crows = [x for (x,) in rows]
        print(crows)
        return crows
        # print(self._pending_services.get(self._get_full_service_name(service, version), []))
        # return self._pending_services.get(self._get_full_service_name(service, version), [])

    def remove_pending_instance(self, service, version, node_id):
        #TODO: update is_pending flag, (assume service gets activated)
        self.qupdate('services', values={'is_pending': False}, where_keys=[{'id': ('=', node_id)}])

        # self.get_pending_instances(service, version).remove(node_id)
        # if not len(self.get_pending_instances(service, version)):
        #     self._pending_services.pop(self._get_full_service_name(service, version))

    def get_instances(self, service, version):
        #TODO: select * from services where service, version
        rows = self.qselect('services', 'port',columns=['ip', 'port', 'id', 'protocol'],
                            where_keys=[{'service_name': ('=', service), 'version': ('=', version)}])
        print("get_instances")
        print(rows)
        return rows
        # print(self._registered_services[service][version])
        # return self._registered_services[service][version]

    def get_versioned_instances(self, service, version):
        #TODO: select * from services where service, version
        rows = self.qselect('services', 'version', columns=['version'], where_keys=[{'service_name': ('=', service)}])
        crows = [x for (x,) in rows]
        version = self._get_non_breaking_version(version, crows)
        rows2 = self.qselect('services', 'port',columns=['ip', 'port', 'id', 'protocol'],
                                       where_keys=[{'service_name': ('=', service), 'version': ('=', version)}])
        print("get_versioned_instances")
        print(rows2)
        return rows2
        # print(self._registered_services[service][version])
        # return self._registered_services[service][version]

    def get_consumers(self, service_name, service_version):
        #TODO: select child.* from dependencies where parent.service, version
        rows = self.qselect('dependencies', 'child_name',columns=['child_name', 'child_version'],
                                      where_keys=[{'parent_name': ('=', service_name),
                                                   'parent_version': ('=', service_version)}])
        print("get_consumers")
        crows = set(rows)
        print(crows)
        return crows
        # consumers = set()
        # for service, vendors in self._service_dependencies.items():
        #     for each in vendors:
        #         if each['service'] == service_name and each['version'] == service_version:
        #             consumers.add(self._split_key(service))
        #
        # print(consumers)
        # return consumers

    def get_vendors(self, service, version):
        #TODO: select child.* from dependencies where service, version
        rows = self.qselect('dependencies', 'parent_name',columns=['parent_name', 'parent_version'],
                                      where_keys=[{'child_name': ('=', service),
                                                   'child_version': ('=', version)}])
        print("get_vendors")
        crows = [{'service': s, 'version': v} for (s,v) in rows]
        return crows
        # print(crows)
        # print(self._service_dependencies.get(self._get_full_service_name(service, version), []))
        # return self._service_dependencies.get(self._get_full_service_name(service, version), [])

    def get_node(self, node_id):
        #TODO: select * from services where node_id
        rows = self.qselect('services', 'service_name',
                            columns=['service_name','version', 'ip', 'port', 'id', 'protocol'],
                            where_keys=[{'id': ('=', node_id)}])
        print(rows)
        for name, version, host, port, node, protocol in rows:
            return Service(name, version, [], host, port, node, protocol)
        # for name, versions in self._registered_services.items():
        #     for version, instances in versions.items():
        #         for host, port, node, service_type in instances:
        #             if node_id == node:
        #                 return Service(name, version, [], host, port, node, service_type)
        return None

    def remove_node(self, node_id):
        #TODO: delete from services where node_id
        self.qdelete('services', where_keys=[{'id': ('=', node_id)}])
        # for name, versions in self._registered_services.items():
        #     for version, instances in versions.items():
        #         for instance in instances:
        #             host, port, node, service_type = instance
        #             if node_id == node:
        #                 instances.remove(instance)
        #TODO: insert into uptimes (node_id, downtime, time.time())
        uptimes_dict = {'id': node_id, 'event_type': 'DOWNTIME', 'event_time': int(time.time())}
        self.qinsert('uptimes', uptimes_dict)
        # for name, nodes in self._uptimes.items():
        #     for node, uptimes in nodes.items():
        #         if node == node_id:
        #             uptimes['downtime'] = int(time.time())
        return None

    def get_uptimes(self):
        #TODO: select * from uptimes
        rows = self.qselect('uptimes', 'id', columns=['id', 'event_type', 'event_time'])
        print("get_uptimes")
        print(rows)
        return rows
        # print(self._uptimes)
        # return self._uptimes

    def xsubscribe(self, service, version, host, port, node_id, endpoints):
        #TODO: insert into subscriptions
        for endpoint in endpoints:
            subscription_dict = {'subscriber_name': service, 'subscriber_version': version,
                                 'subscribee_name': endpoint['service'], 'subscribee_version': endpoint['version'],
                                 'event_name': endpoint['endpoint'], 'strategy': endpoint['strategy']}
            self.qinsert('subscriptions', subscription_dict)

        # entry = (service, version, host, port, node_id)
        # for endpoint in endpoints:
        #     self._subscribe_list[endpoint['service']][endpoint['version']][endpoint['endpoint']].append(
        #         entry + (endpoint['strategy'],))

    def get_subscribers(self, service, version, endpoint):
        #TODO: select * from subscriptions, services where service, version, endpoint
        query = "select subscriber_name, subscriber_version, ip, port, id, strategy " \
                "from services as sv, subscriptions as sb where sb.subscribee_name = %s and " \
                "sb.subscribee_version = %s and sb.event_name = %s and sb.subscriber_name = sv.service_name and " \
                "sb.subscriber_version = sv.version"
        self.cur.execute(query, (service, version, endpoint))
        rows = self.cur.fetchall()
        print("get_subscribers")
        print(rows)
        return rows
        # print(self._subscribe_list[service][version][endpoint])
        # return self._subscribe_list[service][version][endpoint]

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
        rows = self.qselect('services', 'port', columns=['ip', 'port', 'id', 'protocol'])
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
        for (host, port, node, type) in self._repository.get_registered_services():
            try:
                self._connect_to_service(host, port, node, type)
            except Exception as e:
                logger.error(e)

    def start(self):
        setup_logging("registry")
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))
        registry_coroutine = self._loop.create_server(partial(get_vyked_protocol, self), self._ip, self._port)
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
        print(request_type)
        try:
            params = packet['params']
            self._client_protocols[params['node_id']] = protocol
        except:
            try:
                self._client_protocols[packet['node_id']] = protocol
                print(packet['node_id'])
            except:
                # print(request_type)
                print("NO NODE ID")
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

    def deregister_service(self, node_id):
        service = self._repository.get_node(node_id)
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
        if params['node_id'] not in self._service_protocols.keys():
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
                    logger.info('%s activated', (service, version))
                else:
                    logger.info('%s can\'t register because it depends on %s', (service, version), vendor)

    def _make_activated_packet(self, service, version):
        vendors = self._repository.get_vendors(service, version)
        instances = {
            (v['service'], v['version']): self._repository.get_versioned_instances(v['service'], v['version'])
            for v in vendors}
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

    def on_timeout(self, node_id):
        self.deregister_service(node_id)

    def _ping(self, packet):
        pinger = self._pingers[packet['node_id']]
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

if __name__ == '__main__':
    config_logs(enable_ping_logs=False, log_level=logging.DEBUG)
    from setproctitle import setproctitle

    setproctitle("registry")
    REGISTRY_HOST = None
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT, PersistentRepository())
    registry.start()