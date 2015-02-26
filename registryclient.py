from again.utils import unique_hex
from collections import defaultdict


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
        self._assigned_services = {}

    def register(self, dependencies, ip, port, app, service, version):
        self._app = app
        self._service = service
        self._version = version
        packet = self._make_registration_packet(ip, port, app, service, version, dependencies)
        self._protocol.send(packet)

    def _protocol_factory(self):
        from jsonprotocol import RegistryClientProtocol
        p = RegistryClientProtocol(self)
        return p

    def connect(self):
        coro = self._loop.create_connection(self._protocol_factory, self._host, self._port)
        self._transport, self._protocol = self._loop.run_until_complete(coro)

    def receive(self, packet:dict, registry_protocol):
        if packet['type'] == "registered":
            self.cache_vendors(packet['params']['vendors'])
            self._bus.registration_complete()

    def get_all_addresses(self, full_service_name):
        return self._available_services.get(
            self._get_full_service_name(full_service_name[0], full_service_name[1], full_service_name[2]))

    def resolve(self, app: str, service: str, version: str, entity:str):
        entity_map = self._assigned_services.get(self._get_full_service_name(app, service, version))
        return entity_map.get(entity)

    def _make_registration_packet(self, ip:str, port:str, app:str, service:str, version:str, dependencies):
        self._node_id = unique_hex()
        params = {'app': app,
                  'service': service,
                  'version': version,
                  'host': ip,
                  'port': port,
                  'node_id': self._node_id,
                  'dependencies': dependencies}
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

