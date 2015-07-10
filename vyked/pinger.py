import asyncio


class Pinger:
    """
    Pinger to send ping packets to an endpoint and inform if the timeout has occurred
    """
    def __init__(self, handler, interval, timeout, loop=asyncio.get_event_loop()):
        """Aysncio based pinger
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

    @asyncio.coroutine
    def send_ping(self):
        """Sends the ping after the interval specified when initializing"""
        yield from asyncio.sleep(self._interval)
        self._handler.send_ping()
        self._start_timer()

    def pong_received(self):
        """Called when a pong is received. So the timer is cancelled"""
        self._timer.cancel()
        self.send_ping()

    def _start_timer(self):
        self._timer = asyncio.async(asyncio.sleep(self._timeout), loop=self._loop)
        self._handler.on_timeout()
