from asyncio import sleep
import json

from aiohttp.web import Response, Request

from vyked import Host, HTTPService, TCPService, get, post, api, publish

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

IDENTITY_HOST = '127.0.0.1'
IDENTITY_HTTP_PORT = 4501
IDENTITY_TCP_PORT = 4502


class UserException(BaseException):
    pass


class IdentityHTTPService(HTTPService):
    def __init__(self, ip, port):
        super(IdentityHTTPService, self).__init__("IdentityService", 1, ip, port)

    @get(path='/users/{username}')
    def get(self, request: Request):
        username = request.match_info.get('username')
        return Response(status=200, body=("Hello {}".format(username)).encode())

    @post(path='/users/{username}')
    def create(self, request: Request):
        data = yield from request.json()
        return Response(status=200, body=(json.dumps(data)).encode())


class IdentityTCPService(TCPService):
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


if __name__ == '__main__':
    http = IdentityHTTPService(IDENTITY_HOST, IDENTITY_HTTP_PORT)
    tcp = IdentityTCPService(IDENTITY_HOST, IDENTITY_TCP_PORT)
    Host.registry_host = REGISTRY_HOST
    Host.registry_port = REGISTRY_PORT
    Host.pubsub_host = REDIS_HOST
    Host.pubsub_port = REDIS_PORT
    Host.name = 'Identity'
    Host.attach_service(http)
    Host.attach_service(tcp)
    Host.run()
