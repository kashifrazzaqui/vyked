from vyked.services import HTTPServiceClient
from vyked.services import get

import asyncio


class Hello(HTTPServiceClient):
    def __init__(self):
        super(Hello, self).__init__('Hello', 1, 'test')

    @get
    def person(self, url):
        params = {'key': 'value'}
        headers = {'Content-Type': 'application/json'}
        return locals()


def process_response(response):
    body = yield from response.text()
    return body

if __name__ == '__main__':
    hello = Hello()
    url = 'http://127.0.0.1:7890/{}'.format('ankit')
    r = asyncio.get_event_loop().run_until_complete(hello.person(url))
    body = asyncio.get_event_loop().run_until_complete(process_response(r))
    print(body)
