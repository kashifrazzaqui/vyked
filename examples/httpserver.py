from asyncio import coroutine
import aiohttp

from aiohttp.web import Response, Request

from vyked import HTTPApplicationService
from vyked import Bus


REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500


class Hello(HTTPApplicationService):
    def __init__(self):
        super(Hello, self).__init__('Hello', 1, 'test', '127.0.0.1', '7890')

    def get_routes(self) -> list:
        return [('GET', '/', self.root), ('GET', '/{name}', self.person)]

    @coroutine
    def root(self, request:Request) -> Response:
        response = yield from aiohttp.request('get', 'https://github.com/timeline.json')
        result = yield from response.text()
        return Response(body=result.encode())

    def person(self, request:Request) -> Response:
        result = 'Hello' + request.match_info.get('name', 'Anon')
        return Response(body=result.encode())


if __name__ == '__main__':
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    hello = Hello()
    hello.ronin = True
    bus.serve_http(hello)
    bus.start()
