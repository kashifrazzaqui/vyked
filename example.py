from bus import Bus
from registry import Registry
from services import ServiceHost, ServiceClient, api, publish, request, subscribe

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

IDENTITY_HOST = '127.0.0.1'
IDENTITY_PORT = 4501

class IdentityService(ServiceHost):

    def __init__(self):
        super(IdentityService, self).__init__("IdentityService", "1", "Example")

    @api
    def create(self, user_name, password):
        return user_name

    @publish
    def password_changed(self, user_name):
        pass


class IdentityClient(ServiceClient):
    @request
    def create(self, user_name, password):
        return locals()

    @subscribe
    def password_changed(self, user_name):
        pass


def setup_registry():
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()


def setup_identity_service():
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    identity_service = IdentityService()
    bus.serve(identity_service)
    bus.start(IDENTITY_HOST, IDENTITY_PORT)
    bus.start(IDENTITY_HOST, IDENTITY_PORT)

if __name__ == '__main__':
    setup_registry()
    setup_identity_service()

