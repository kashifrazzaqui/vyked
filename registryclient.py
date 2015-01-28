from jsonprotocol import RegistryProtocol


class RegistryClient:
    def __init__(self, loop, host, port):
        self._loop = loop
        self._host = host
        self._port = port
        self._transport = None
        self._protocol = None

    def host(self, app, service, version):
        # TODO: register service host
        pass

    def _protocol_factory(self):
        p = RegistryProtocol(self)
        return p

    def connect(self):
        coro = self._loop.create_connection(self._protocol_factory, self._host, self._port)
        self._transport, self._protocol = self._loop.run_until_complete(coro)

    def provision(self, full_service_names):
        for app_name, service_name, version in full_service_names:
            pass
        # ask registry for all instance endpoints for given service names
        # returns a future whose result is set when registry protocol receives a response
        # which it sends to RegistryClient._receive which then sets result on this future
        # Note: registry returns a 3 tuple of host, port and node_id for each service provisioned
        pass

    def _receive(self):
        # called from registry protocol when it gets data
        pass

    def get_all_addresses(self, full_service_name):
        # returns a list of all (host, port, node_id) that point to instances of given service
        # this info comes from the registry after registering
        pass

    def resolve(self, app: str, service: str, version: str, entity:str):
        pass
