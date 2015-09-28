import asyncio

from aiohttp import request

from vyked.packet import ControlPacket
import logging

PING_TIMEOUT = 10
PING_INTERVAL = 5


class Pinger:
    """
    Pinger to send ping packets to an endpoint and inform if the timeout has occurred
    """

    def __init__(self, handler, interval, timeout, loop=asyncio.get_event_loop(), max_failures=5):
        """
        Aysncio based pinger
        :param handler: Pinger uses it to send a ping and inform when timeout occurs.
                        Must implement send_ping() and on_timeout() methods
        :param int interval: time interval between ping after a pong
        :param loop: Optional event loop
        """

        self._handler = handler
        self._interval = interval
        self._timeout = timeout
        self._loop = loop
        self._timer = None
        self._failures = 0
        self._max_failures = max_failures

    @asyncio.coroutine
    def send_ping(self):
        """
        Sends the ping after the interval specified when initializing
        """
        yield from asyncio.sleep(self._interval)
        self._handler.send_ping()
        self._start_timer()

    def pong_received(self):
        """
        Called when a pong is received. So the timer is cancelled
        """
        if self._timer is not None:
            self._timer.cancel()
            self._failures = 0
            asyncio.async(self.send_ping())

    def _start_timer(self):
        self._timer = self._loop.call_later(self._timeout, self._on_timeout)

    def stop(self):
        if self._timer is not None:
            self._timer.cancel()

    def _on_timeout(self):
        if self._failures < self._max_failures:
            self._failures += 1
            asyncio.async(self.send_ping())
        else:
            self._handler.on_timeout()


class TCPPinger:
    logger = logging.getLogger(__name__)

    def __init__(self, host, port, node_id, protocol, handler):
        self._host = host
        self._port = port
        self._pinger = Pinger(self, PING_INTERVAL, PING_TIMEOUT)
        self._node_id = node_id
        self._protocol = protocol
        self._handler = handler

    def ping(self):
        asyncio.async(self._pinger.send_ping())

    def send_ping(self):
        self._protocol.send(ControlPacket.ping(self._node_id))

    def on_timeout(self):
        self.logger.debug('%s timed out', self._node_id)
        # Dummy packet to cleanly close transport
        self._protocol._transport.write(
            '{"closed":"true", "type":"closed", "service":"none", "version":"none"}'.encode())
        self._protocol.close()
        self._handler.on_timeout(self._host, self._port, self._node_id)

    def stop(self):
        self._pinger.stop()

    def pong_received(self):
        self._pinger.pong_received()


class HTTPPinger:

    def __init__(self, host, port, node_id, handler):
        self._host = host
        self._port = port
        self._pinger = Pinger(self, PING_INTERVAL, PING_TIMEOUT)
        self._node_id = node_id
        self._handler = handler
        self._url = 'http://{}:{}/ping'.format(host, port)
        self.logger = logging.getLogger(__name__)

    def ping(self):
        asyncio.async(self._pinger.send_ping())

    def send_ping(self):
        asyncio.async(self.ping_coroutine())

    def ping_coroutine(self):
        res = yield from request('get', self._url)
        if res.status == 200:
            self.pong_received()
            res.close()

    def stop(self):
        self._pinger.stop()

    def on_timeout(self):
        self.logger.debug('%s timed out', self._node_id)
        self._handler.on_timeout(self._host, self._port, self._node_id)

    def pong_received(self):
        self._pinger.pong_received()
