from asyncio import coroutine, sleep
import asyncio
from aiohttp.web import Response

from vyked import Bus, get
from vyked import TCPApplicationService, HTTPApplicationService, api, publish


REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

IDENTITY_HOST = '127.0.0.1'
IDENTITY_HTTP_PORT = 4501
IDENTITY_TCP_PORT = 4502



class UserException(BaseException):
    pass


class IdentityHTTPService(HTTPApplicationService):
    def __init__(self, ip, port):
        super(IdentityHTTPService, self).__init__("IdentityService", 1, ip, port)

    @get(path='/')
    def create(self, request):
        return Response()


class IdentityTCPService(TCPApplicationService):
    def __init__(self, ip, port):
        super(IdentityTCPService, self).__init__("IdentityService", 1, ip, port)

    @api
    @coroutine
    def create(self, user_name, password):
        result = yield from self._create_user_name(user_name, password)
        if user_name is None:
            raise UserException('username cannot be none')
        return result

    @publish
    def password_changed(self, user_name):
        return locals()

    @staticmethod
    def _create_user_name(user_name, password):
        yield from sleep(5)
        return '{} {}'.format(user_name, password)


def setup_identity_service():
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    http = IdentityHTTPService(IDENTITY_HOST, IDENTITY_HTTP_PORT)
    tcp = IdentityTCPService(IDENTITY_HOST, IDENTITY_TCP_PORT)
    bus.serve_http(http)
    bus.serve_tcp(tcp)
    asyncio.get_event_loop().call_later(10, tcp.password_changed, "username")
    bus.start()


if __name__ == '__main__':
    setup_identity_service()

