import logging
import asyncio

import aiohttp

DEFAULT_TIMEOUT = 15
DEFAULT_PING_INTERVAL= 10


class Pinger:
    _logger = logging.getLogger(__name__)

    def __init__(self, ping_handler, loop, timeout=DEFAULT_TIMEOUT):
        self._ping_handler = ping_handler
        self._loop = loop
        self._count = 0
        self._timeout = timeout
        self._interval = DEFAULT_PING_INTERVAL
        self._timer = None
        self._tcp = None
        self._protocol = None
        self._node = None
        self._host = None
        self._port = None

    def register_tcp_service(self, protocol, node):
        self._tcp = True
        self._protocol = protocol
        self._node = node

    def register_http_service(self, host, port, node):
        self._tcp = False
        self._host = host
        self._port = port
        self._node = node

    @asyncio.coroutine
    def start_ping(self):
        if self._tcp:
            self._timer = self._loop.call_later(self._interval, self._send_timed_ping)
        else:
            url = 'http://{}:{}/ping'.format(self._host, self._port)
            yield from asyncio.sleep(self._interval)
            self._logger.debug('Pinging node {} at url: {}'.format(self._node, url))
            try:
                resp = yield from asyncio.wait_for(aiohttp.request('GET', url=url), self._interval)
                resp.close()
                yield from self.start_ping()
            except:
                self._ping_handler.handle_ping_timeout(self._node)
                pass

    def pong_received(self, count):
        if self._timer is not None:
            self._timer.cancel()

        if self._count == count:
            self._count += 1
            yield from self.start_ping()
        else:
            self._ping_timed_out()

    def _make_ping_packet(self):
        packet = {'type': 'ping', 'node_id': self._node, 'count': self._count}
        return packet

    def _send_timed_ping(self):
        packet = self._make_ping_packet()
        self._protocol.send(packet)
        self._timer = self._loop.call_later(self._timeout, self._ping_timed_out)

    def _ping_timed_out(self):
        self._ping_handler.handle_ping_timeout(self._node)
