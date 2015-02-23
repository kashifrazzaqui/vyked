from again.utils import unique_hex

class RegistryClient:
    def __init__(self, loop, host, port):
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
        self._available_services = {}
        self._assigned_services = {}

    def register(self, dependencies, app, service, version):
        self._app = app
        self._service = service
        self._version = version
        packet = self._make_registration_packet(app, service, version, dependencies)
        self._protocol.send(packet)

    def _protocol_factory(self):
        from jsonprotocol import RegistryClientProtocol
        p = RegistryClientProtocol(self)
        return p

    def connect(self):
        coro = self._loop.create_connection(self._protocol_factory, self._host, self._port)
        self._transport, self._protocol = self._loop.run_until_complete(coro)

    def receive(self, packet:dict, registry_protocol):
        # handle responses from protocol
        pass

    def get_all_addresses(self, full_service_name):
        return self._available_services.get(
            self._get_full_service_name(full_service_name[0], full_service_name[1], full_service_name[2]))

    def resolve(self, app: str, service: str, version: str, entity:str):
        entity_map = self._assigned_services.get(self._get_full_service_name(app, service, version))
        return entity_map.get(entity)

    def _make_registration_packet(self, app:str, service:str, version:str, dependencies):
        self._node_id = unique_hex()
        params = {'app': app,
                  'service': service,
                  'version': version,
                  'host': self._host,
                  'port': self._port,
                  'node_id': self._node_id,
                  'dependencies': dependencies}
        packet = {'pid': unique_hex(),
                  'type': 'register',
                  'params': params}
        return packet

    @staticmethod
    def _get_full_service_name(app, service, version):
        return "{}/{}/{}".format(app, service, version)
