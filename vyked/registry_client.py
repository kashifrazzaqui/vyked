import asyncio
import logging
import random
from collections import defaultdict

from again.utils import unique_hex
from functools import partial

from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol


class RegistryClient:
    logger = logging.getLogger(__name__)

    def __init__(self, loop, host, port):
        self._loop = loop
        self._port = port
        self._host = host
        self.bus = None
        self._service_host = None
        self._service_port = None
        self._transport = None
        self._protocol = None
        self._service = None
        self._version = None
        self._node_id = None
        self._pending_requests = {}
        self._available_services = defaultdict(list)
        self._assigned_services = defaultdict(lambda: defaultdict(list))

    def register(self, ip, port, service, version, vendors, service_type):
        self._service_host = ip
        self._service_port = port
        self._service = service
        self._version = version
        self._node_id = '{}_{}_{}'.format(service, version, unique_hex())
        packet = ControlPacket.registration(ip, port, self._node_id, service, version, vendors, service_type)
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

    def x_subscribe(self, endpoints):
        packet = ControlPacket.xsubscribe(self._service, self._version, self._service_host, self._service_port, self._node_id,
                                          endpoints)
        self._protocol.send(packet)

    def connect(self):
        coroutine = self._loop.create_connection(partial(get_vyked_protocol, self), self._host, self._port)
        self._transport, self._protocol = self._loop.run_until_complete(coroutine)

    def receive(self, packet: dict, protocol, transport):
        if packet['type'] == 'registered':
            self.cache_vendors(packet['params']['vendors'])
            self.bus.registration_complete()
        elif packet['type'] == 'deregister':
            self._handle_deregistration(packet)
        elif packet['type'] == 'subscribers':
            self._handle_subscriber_packet(packet)

    def get_all_addresses(self, full_service_name):
        return self._available_services.get(
            self._get_full_service_name(full_service_name[0], full_service_name[1]))

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

    def _handle_deregistration(self, packet):
        params = packet['params']
        vendor = params['vendor']
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

    def _handle_subscriber_packet(self, packet):
        request_id = packet['request_id']
        future = self._pending_requests[request_id]
        future.set_result(packet['params']['subscribers'])
