from aiohttp.web import Response

from vyked import HTTPServiceClient, HTTPApplicationService, Bus
from vyked import get


class Hello(HTTPServiceClient):
    def __init__(self):
        super(Hello, self).__init__('Hello', 1)

    @get()
    def person(self, name):
        path = '/{}'.format(name)
        params = {'key': 'value'}
        headers = {'Content-Type': 'application/json'}
        app_name = 'test'
        return locals()


def process_response(response):
    body = yield from response.text()
    return body


class TestService(HTTPApplicationService):

    def __init__(self, host, port):
        super(TestService, self).__init__("Test", 1, host, port)
        self._client = None

    def set_client(self, hello_client):
        self._client = hello_client

    @get('/test')
    def get_res(self, request):
        url = 'ankit'
        yield from self._client.person(url)
        return Response()

if __name__ == '__main__':
    ts = TestService('127.0.0.1', 4502)
    client = Hello()
    ts.set_client(client)
    bus = Bus('127.0.0.1', 4500)
    bus.serve_http(ts)
    bus.require([client])
    bus.start()
