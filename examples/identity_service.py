from vyked.bus import Bus
from vyked.services import TCPApplicationService, TCPServiceClient, api, publish, request, subscribe

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

IDENTITY_HOST = '127.0.0.1'
IDENTITY_PORT = 4501


class IdentityService(TCPApplicationService):
    def __init__(self, ip, port):
        super(IdentityService, self).__init__("IdentityService", "1", "Example", ip, port)

    @api
    def create(self, user_name, password):
        return user_name

    @publish
    def password_changed(self, user_name):
        pass


class IdentityClient(TCPServiceClient):
    def __init__(self):
        super(IdentityClient, self).__init__("IdentityService", "1", "Example")

    @request
    def create(self, user_name, password):
        return locals()

    @subscribe
    def password_changed(self, user_name):
        pass


def setup_identity_service():
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    identity_service = IdentityService(IDENTITY_HOST, IDENTITY_PORT)
    bus.serve_tcp(identity_service)
    bus.start()


if __name__ == '__main__':
    setup_identity_service()

