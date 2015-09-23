import asyncio
import logging
import random
from collections import defaultdict
from functools import partial

from retrial.retrial import retry

from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol
from .pinger import TCPPinger


def _retry_for_result(result):
    if isinstance(result, tuple):
        return not isinstance(result[0], asyncio.transports.Transport) or not isinstance(result[1], asyncio.Protocol)
    return True


def _retry_for_exception(_):
    return True


class RegistryClient:

    def __init__(self, loop, host, port, ssl_context=None):
        self._loop = loop
        self._port = port
        self._host = host
        self.bus = None
        self._transport = None
        self._protocol = None
        self._service = None
        self._version = None
        self._pinger = None
        self._conn_handler = None
        self._pending_requests = {}
        self._available_services = defaultdict(list)
        self._assigned_services = defaultdict(lambda: defaultdict(list))
        self._ssl_context = ssl_context
        self.logger = logging.getLogger(__name__)

    @property
    def conn_handler(self):
        return self._conn_handler

    @conn_handler.setter
    def conn_handler(self, handler):
        self._conn_handler = handler

    def register(self, ip, port, service, version, node_id, vendors, service_type):
        self._service = service
        self._version = version
        packet = ControlPacket.registration(ip, port, node_id, service, version, vendors, service_type)
        self._protocol.send(packet)

    def get_instances(self, service, version):
        packet = ControlPacket.get_instances(service, version)
        future = asyncio.Future()
        self._protocol.send(packet)
        self._pending_requests[packet['request_id']] = future
        return future

    def get_subscribers(self, service, version, endpoint):
        packet = ControlPacket.get_subscribers(service, version, endpoint)
        # TODO : remove duplication in get_instances and get_subscribers
        future = asyncio.Future()
        self._protocol.send(packet)
        self._pending_requests[packet['request_id']] = future
        return future

    def x_subscribe(self, host, port, node_id, endpoints):
        packet = ControlPacket.xsubscribe(self._service, self._version, host, port, node_id,
                                          endpoints)
        self._protocol.send(packet)

    @retry(should_retry_for_result=_retry_for_result, should_retry_for_exception=_retry_for_exception,
           strategy=[0, 2, 4, 8, 16, 32])
    def connect(self):
        self._transport, self._protocol = yield from self._loop.create_connection(partial(get_vyked_protocol, self),
                                                                                  self._host, self._port,
                                                                                  ssl=self._ssl_context)
        self.conn_handler.handle_connected()
        if self._pinger:
            self._pinger.stop()
        self._pinger = TCPPinger(self._host, self._port, 'registry', self._protocol, self)
        self._pinger.ping()
        return self._transport, self._protocol

    def on_timeout(self, host, port, node_id):
        asyncio.async(self.connect())

    def receive(self, packet: dict, protocol, transport):
        if packet['type'] == 'registered':
            self.cache_vendors(packet['params']['vendors'])
            self.bus.registration_complete()
        elif packet['type'] == 'new_instance':
            # TODO : once method for both vendors and new instance
            self.cache_instance(**packet['params'])
            self._handle_new_instance(**packet['params'])
        elif packet['type'] == 'deregister':
            self._handle_deregistration(packet)
        elif packet['type'] == 'subscribers':
            self._handle_subscriber_packet(packet)
        elif packet['type'] == 'pong':
            self._pinger.pong_received()
        elif packet['type'] == 'instances':
            self._handle_get_instances(packet)

    def get_all_addresses(self, name, version):
        return self._available_services.get(
            self._get_full_service_name(name, version))

    def get_for_node(self, node_id):
        for services in self._available_services.values():
            for host, port, node, service_type in services:
                if node == node_id:
                    return host, port, node, service_type
        return None

    def get_random_service(self, service_name, service_type):
        services = self._available_services[service_name]
        services = [service for service in services if service[3] == service_type]
        if len(services):
            return random.choice(services)
        else:
            return None

    def resolve(self, service: str, version: str, entity: str, service_type: str):
        service_name = self._get_full_service_name(service, version)
        if entity is not None:
            entity_map = self._assigned_services.get(service_name)
            if entity_map is None:
                self._assigned_services[service_name] = {}
            entity_map = self._assigned_services.get(service_name)
            if entity in entity_map:
                return entity_map[entity]
            else:
                host, port, node_id, service_type = self.get_random_service(service_name, service_type)
                if node_id is not None:
                    entity_map[entity] = host, port, node_id, service_type
                return host, port, node_id, service_type
        else:
            return self.get_random_service(service_name, service_type)

    @staticmethod
    def _get_full_service_name(service, version):
        return "{}/{}".format(service, version)

    def cache_vendors(self, vendors):
        for vendor in vendors:
            vendor_name = self._get_full_service_name(vendor['name'], vendor['version'])
            for address in vendor['addresses']:
                self._available_services[vendor_name].append(
                    (address['host'], address['port'], address['node_id'], address['type']))
        self.logger.debug('Connection cache after registration is %s', self._available_services)

    def cache_instance(self, service, version, host, port, node, type):
        vendor = self._get_full_service_name(service, version)
        self._available_services[vendor].append((host, port, node, type))
        self.logger.debug('Connection cache on getting new instance is %s', self._available_services)

    def _handle_deregistration(self, packet):
        params = packet['params']
        vendor = self._get_full_service_name(params['service'], params['version'])
        node = params['node_id']
        for each in self._available_services[vendor]:
            if each[2] == node:
                self._available_services[vendor].remove(each)
        entity_map = self._assigned_services.get(vendor)
        if entity_map is not None:
            stale_entities = []
            for entity, node_id in entity_map.items():
                if node == node_id:
                    stale_entities.append(entity)
            for entity in stale_entities:
                entity_map.pop(entity)
        self.logger.debug('Connection cache after deregister is %s', self._available_services)

    def _handle_subscriber_packet(self, packet):
        request_id = packet['request_id']
        future = self._pending_requests.pop(request_id, None)
        future.set_result(packet['params']['subscribers'])

    def _handle_get_instances(self, packet):
        future = self._pending_requests.pop(packet['request_id'], None)
        future.set_result(packet['params']['instances'])

    def _handle_new_instance(self, service, version, host, port, node, type):
        self.bus.new_instance(service, version, host, port, node, type)
