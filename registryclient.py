
class RegistryClient:
    def get_endpoint(self):
        # TODO: connect to service registry
        pass

    def add(self, _host):
        # TODO: register service host
        pass

    def connect(self):
        # asyncio.create_connection and run_until_complete; blocking
        pass

    def provision(self, service_names):
        # ask registry for all instance endpoints for given service names
        # returns a future whose result is set when registry protocol receives a response
        pass

    def _receive(self):
        # called from registry protocol when it gets data
        pass
