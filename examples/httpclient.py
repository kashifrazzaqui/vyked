from vyked.services import HTTPServiceClient
from vyked.services import http_request

import asyncio


class Hello(HTTPServiceClient):
    def __init__(self):
        super(Hello, self).__init__('Hello', 1, 'test')

    @http_request
    def person(self, name):
        method = 'get'
        url = 'http://127.0.0.1:7890/{}'.format(name)
        params = {'key': 'value'}
        return locals()


def process_response(response):
    body = yield from response.text()
    return body

if __name__ == '__main__':
    hello = Hello()
    r = asyncio.get_event_loop().run_until_complete(hello.person('user'))
    body = asyncio.get_event_loop().run_until_complete(process_response(r))
    print(body)
