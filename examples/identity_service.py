from asyncio import sleep
import json

from aiohttp.web import Response, Request

from vyked import Bus, get, post
from vyked import TCPApplicationService, HTTPApplicationService, api, publish

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

IDENTITY_HOST = '127.0.0.1'
IDENTITY_HTTP_PORT = 4501
IDENTITY_TCP_PORT = 4502


class UserException(BaseException):
    pass


class IdentityHTTPService(HTTPApplicationService):
    def __init__(self, ip, port):
        super(IdentityHTTPService, self).__init__("IdentityService", 1, ip, port)

    @get(path='/users/{username}')
    def create(self, request: Request):
        username = request.match_info.get('username')
        return Response(status=200, body=("Hello {}".format(username)).encode())

    @post(path='/users/{username}')
    def create(self, request: Request):
        data = yield from request.json()
        return Response(status=200, body=(json.dumps(data)).encode())

class IdentityTCPService(TCPApplicationService):
    def __init__(self, ip, port):
        super(IdentityTCPService, self).__init__("IdentityService", 1, ip, port)

    @api
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
    bus = Bus()
    http = IdentityHTTPService(IDENTITY_HOST, IDENTITY_HTTP_PORT)
    tcp = IdentityTCPService(IDENTITY_HOST, IDENTITY_TCP_PORT)
    bus.serve_http(http)
    bus.serve_tcp(tcp)
    bus.start(REGISTRY_HOST, REGISTRY_PORT, REDIS_HOST, REDIS_PORT)


if __name__ == '__main__':
    setup_identity_service()

