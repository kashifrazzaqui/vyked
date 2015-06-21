import aiohttp
from aiohttp.web import Response

from vyked import HTTPApplicationService, get

from vyked import Bus


REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500


class Hello(HTTPApplicationService):
    def __init__(self):
        super(Hello, self).__init__('Hello', 1, '127.0.0.1', '7890')

    @get(path='/')
    def root(self, request) -> Response:
        response = yield from aiohttp.request('get', 'https://github.com/timeline.json')
        result = yield from response.text()
        return Response(body=result.encode())

    @get(path='/{name}', required_params=['test', 'name'])
    def person(self, request) -> Response:
        result = 'Hello' + request.match_info.get('name', 'Anon')
        return Response(body=result.encode())


if __name__ == '__main__':
    bus = Bus()
    hello = Hello()
    hello.ronin = True
    bus.serve_http(hello)
    bus.start(REGISTRY_HOST, REGISTRY_PORT)
