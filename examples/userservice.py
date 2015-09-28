from asyncio import sleep
import json

from vyked import Host, HTTPService, TCPService, get, post, api, publish, Request, Response

REGISTRY_PORT = 4500
REDIS_PORT = 6379
USER_HOST = '127.0.0.1'
USER_HTTP_PORT = 4501
USER_TCP_PORT = 4502


class UserException(BaseException):
    pass


class UserHTTPService(HTTPService):
    def __init__(self, ip, port):
        super(UserHTTPService, self).__init__("UserService", 1, ip, port)

    @get(path='/users/{username}')
    def get(self, request: Request):
        username = request.match_info.get('username')
        return Response(status=200, body=("Hello {}".format(username)).encode())

    @post(path='/users/{username}')
    def create(self, request: Request):
        data = yield from request.json()
        return Response(status=200, body=(json.dumps(data)).encode())


class UserTCPService(TCPService):
    def __init__(self, ip, port):
        super(UserTCPService, self).__init__("UserService", 1, ip, port)

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
        yield from sleep(1)
        return '{} {}'.format(user_name, password)


if __name__ == '__main__':
    http = UserHTTPService(USER_HOST, USER_HTTP_PORT)
    tcp = UserTCPService(USER_HOST, USER_TCP_PORT)
    Host.configure('User')
    Host.attach_http_service(http)
    Host.attach_tcp_service(tcp)
    Host.run()
