class Pinger:

    def __init__(self, registry, loop, protocol, node):
        self._registry = registry
        self._protocol = protocol
        self._node = node
        self._loop = loop
        self._count = 0
        self._timer = None

    def start_ping(self):
        packet = self._make_ping_packet()
        self._loop.call_later(10, self._send_timed_ping, packet)

    def pong_received(self, count):
        if self._count == count:
            self._count = self._count + 1
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