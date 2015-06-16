import logging
import asyncio

import aiohttp

class Pinger:

    _logger = logging.getLogger(__name__)

    def __init__(self, registry, loop):
        self._count = 0
        self._timer = None
        self._registry = registry
        self._loop = loop

    def register_tcp_service(self, protocol, node):
        self._tcp = True
        self._protocol = protocol
        self._node = node

    def register_http_service(self, host, port, node):
        self._tcp = False
        self._host = host
        self._port = port
        self._node = node

    def start_ping(self):
        if self._tcp:
            packet = self._make_ping_packet()
            self._loop.call_later(10, self._send_timed_ping, packet)
        else:
            url = 'http://{}:{}/ping'.format(self._host, self._port)
            yield from asyncio.sleep(10)
            self._logger.debug('Pinging node {} at url: {}'.format(self._node, url))
            try:
                yield from asyncio.wait_for(aiohttp.request('GET', url=url), 5)
                yield from self.start_ping()
            except:
                self._registry.handle_ping_timeout(self._node)

    def pong_received(self, count):
        if self._count == count:
            self._count += 1
            if self._timer is not None:
                self._timer.cancel()
        else:
            self._ping_timed_out()

    def _make_ping_packet(self):
        packet = {'type': 'ping', 'node_id': self._node, 'count': self._count}
        return packet

    def _send_timed_ping(self, packet):
        self._protocol.send(packet)
        self._timer = self._loop.call_later(5, self._ping_timed_out)

    def _ping_timed_out(self):
        self._timer.cancel()
        self._registry.handle_ping_timeout(self._node)
