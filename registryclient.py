from asyncio import Future

from again.utils import unique_hex

from jsonprotocol import RegistryClientProtocol


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

    def host(self, app, service, version):
        self._app = app
        self._service = service
        self._version = version
        packet = self._make_host_packet(app, service, version)
        self._protocol.send(packet)

    def _protocol_factory(self):
        p = RegistryClientProtocol(self)
        return p

    def connect(self):
        coro = self._loop.create_connection(self._protocol_factory, self._host, self._port)
        self._transport, self._protocol = self._loop.run_until_complete(coro)

    def provision(self, full_service_names):
        future = Future()
        request_id = unique_hex()
        self._pending_requests[request_id] = future
        packet = self._make_provision_packet(request_id, full_service_names)
        self._protocol.send(packet)
        return future

    def receive(self, packet:dict, registry_protocol:RegistryClientProtocol):
        if packet['type'] == 'provision':
            self._handle_provision_response(packet)

    def get_all_addresses(self, full_service_name):
        return self._available_services.get(
            self._get_full_service_name(full_service_name[0], full_service_name[1], full_service_name[2]))

    def resolve(self, app: str, service: str, version: str, entity:str):
        entity_map = self._assigned_services.get(self._get_full_service_name(app, service, version))
        return entity_map.get(entity)

    def register(self):
        pass

    def _make_host_packet(self, app:str, service:str, version:str):
        self._node_id = unique_hex()
        params = {'app': app,
                  'service': service,
                  'version': version,
                  'host': self._host,
                  'port': self._port,
                  'node_id': self._node_id}
        packet = {'pid': unique_hex(),
                  'type': 'host',
                  'params': params}
        return packet

    def _handle_provision_response(self, packet):
        params = packet['params']
        future = self._pending_requests.pop(params['request_id'])
        self._available_services = params['result']
        future.set_result(packet['result'])

    def _make_provision_packet(self, request_id, full_service_names):
        params = {'app': self._app,
                  'service': self._service,
                  'version': self._version,
                  'service_names': full_service_names,
                  'request_id': request_id}
        packet = {'pid': unique_hex(),
                  'type': 'host',
                  'params': params}
        return packet

    @staticmethod
    def _get_full_service_name(app, service, version):
        return "{}/{}/{}".format(app, service, version)
