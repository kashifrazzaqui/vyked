from asyncio import coroutine, sleep

from vyked import Bus
from vyked import TCPApplicationService, api, publish


REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

IDENTITY_HOST = '127.0.0.1'
IDENTITY_PORT = 4501


class IdentityService(TCPApplicationService):
    def __init__(self, ip, port):
        super(IdentityService, self).__init__("IdentityService", 1, "Example", ip, port)

    @api
    @coroutine
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

