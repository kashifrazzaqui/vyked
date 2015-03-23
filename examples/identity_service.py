from vyked.bus import Bus
from vyked.services import TCPApplicationService, TCPServiceClient, api, publish, request, subscribe
from asyncio import sleep

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

IDENTITY_HOST = '127.0.0.1'
IDENTITY_PORT = 4501


class IdentityService(TCPApplicationService):
    def __init__(self, ip, port):
        super(IdentityService, self).__init__("IdentityService", 1, "Example", ip, port)

    @api
    def create(self, user_name, password):
        result = yield from self._create_user_name(user_name, password)
        return result

    @publish
    def password_changed(self, user_name):
        pass

    @staticmethod
    def _create_user_name(user_name, password):
        yield from sleep(5)
        return '{} {}'.format(user_name, password)


def setup_identity_service():
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    identity_service = IdentityService(IDENTITY_HOST, IDENTITY_PORT)
    identity_service.bus = bus
    bus.serve_tcp(identity_service)
    bus.start()


if __name__ == '__main__':
    setup_identity_service()

