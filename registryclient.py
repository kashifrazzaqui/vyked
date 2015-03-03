from asyncio import Future
import random
from again.utils import unique_hex
from collections import defaultdict
from services import TCPServiceClient


class RegistryClient:
    def __init__(self, loop, host, port, bus):
        self._bus = bus
        self._loop = loop
        self._host = host
        self._port = port
        self._transport = None
        self._protocol = None
        self._app = None
        self._service = None
        self._version = None
        self._node_id = None
        self._pending_requests = {}
        self._available_services = defaultdict(list)
        self._assigned_services = defaultdict(lambda: defaultdict(list))

    def register(self, vendors, ip, port, app, service, version):
        self._app = app
        self._service = service
        self._version = version
        packet = self._make_registration_packet(ip, port, app, service, version, vendors)
        self._protocol.send(packet)
        self._register_for_subscription(vendors, ip, port)

    def _protocol_factory(self):
        from jsonprotocol import RegistryClientProtocol

        p = RegistryClientProtocol(self)
        return p

    def connect(self):
        coro = self._loop.create_connection(self._protocol_factory, self._host, self._port)
        self._transport, self._protocol = self._loop.run_until_complete(coro)

    def receive(self, packet:dict, registry_protocol):
        if packet['type'] == 'registered':
            self.cache_vendors(packet['params']['vendors'])
            self._bus.registration_complete()
        elif packet['type'] == 'deregister':
            self._handle_deregistration(packet)
        elif packet['type'] == 'subscription_list':
            self._handle_subscription_list(packet)

    def get_all_addresses(self, full_service_name):
        return self._available_services.get(
            self._get_full_service_name(full_service_name[0], full_service_name[1], full_service_name[2]))

    def resolve(self, app: str, service: str, version: str, entity:str):
        service_name = self._get_full_service_name(app, service, version)
        entity_map = self._assigned_services.get(service_name)
        if entity_map is None:
            self._assigned_services[service_name] = {}
        entity_map = self._assigned_services.get(service_name)
        if entity in entity_map:
            return entity_map[entity]
        else:
            services = self._available_services[service_name]
            if len(services):
                host, port, node_id = random.choice(services)
                entity_map[entity] = node_id
                return node_id
            else:
                return None

    def _make_registration_packet(self, ip:str, port:str, app:str, service:str, version:str, vendors):
        vendors_list = []
        for vendor in vendors:
            vendor_dict = {'app': vendor.app_name,
                           'service': vendor.name,
                           'version': vendor.version}
            vendors_list.append(vendor_dict)
        self._node_id = unique_hex()
        params = {'app': app,
                  'service': service,
                  'version': version,
                  'host': ip,
                  'port': port,
                  'node_id': self._node_id,
                  'vendors': vendors_list}
        packet = {'pid': unique_hex(),
                  'type': 'register',
                  'params': params}
        return packet

    @staticmethod
    def _get_full_service_name(app, service, version):
        return "{}/{}/{}".format(app, service, version)

    def cache_vendors(self, vendors):
        for vendor in vendors:
            vendor_name = vendor['name']
            for address in vendor['addresses']:
                self._available_services[vendor_name].append((address['host'], address['port'], address['node_id']))

    def _register_for_subscription(self, vendors, ip, port):
        subscription_packet = {
            'type': 'subscribe',
            }
        params = {
            'ip': ip,
            'port': port,
            'node_id': self._node_id
        }
        subscription_list = []
        for vendor in vendors:
            if isinstance(vendor, TCPServiceClient):
                for each in dir(vendor):
                    fn = getattr(vendor, each)
                    if callable(fn) and getattr(fn, 'is_subscribe', False):
                        subscription_list.append({
                            'app': vendor.app_name,
                            'service': vendor.name,
                            'version': vendor.version,
                            'endpoint': fn.__name__
                        })
        params['subscribe_to'] = subscription_list
        subscription_packet['params'] = params
        self._protocol.send(subscription_packet)

    def resolve_publication(self, app, service, version, endpoint):
        future = Future()
        request_id = unique_hex()
        packet = {'type': 'resolve_publication', 'request_id': request_id}
        params = {
            'app': app,
            'service': service,
            'version': version,
            'endpoint': endpoint
        }
        packet['params'] = params
        self._pending_requests[request_id] = future
        self._protocol.send(packet)
        return future

    def _handle_subscription_list(self, packet):
        future = self._pending_requests.pop(packet['request_id'])
        future.set_result(packet['nodes'])

    def _handle_deregistration(self, packet):
        params = packet['params']
        vendor = params['vendor']
        node = params['node_id']
        vendor_name = self._get_full_service_name(vendor['app'], vendor['service'], vendor['version'])
        for each in self._available_services[vendor_name]:
            if each[2] == node:
                self._available_services[vendor_name].remove(each)
        entity_map = self._assigned_services.get(vendor_name)
        print(self._assigned_services)
        if entity_map is not None:
            for entity, node_id in entity_map.items():
                if node == node_id:
                    entity_map.pop(entity)






