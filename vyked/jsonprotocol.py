import asyncio
import json
import logging

from jsonstreamer import ObjectStreamer

from .sendqueue import SendQueue


class JSONProtocol(asyncio.Protocol):

    logger = logging.getLogger(__name__)

    def __init__(self):
        self._send_q = None
        self._connected = False
        self._transport = None
        self._obj_streamer = None

    @staticmethod
    def _make_frame(packet):
        string = json.dumps(packet) + ','
        return string.encode()

    @property
    def is_connected(self):
        return self._connected

    def _write_pending_data(self):
        for packet in self._pending_data:
            frame = self._make_frame(packet)
            self._transport.write(frame.encode())
        self._pending_data.clear()

    def connection_made(self, transport):
        self._connected = True
        self._transport = transport
        self._obj_streamer = ObjectStreamer()
        self._obj_streamer.auto_listen(self, prefix='on_')

        self._transport.send = self._transport.write
        self._send_q = SendQueue(transport, self.is_connected)

        self._transport.write('['.encode())  # start a json array
        self._send_q.send()

    def connection_lost(self, exc):
        self._connected = False
        self.logger.info('Peer closed', self._transport.get_extra_info('peername'))

    def send(self, packet: dict):
        self._send_q.send(self._make_frame(packet))

    def close(self):
        self._transport.write(']'.encode())  # end the json array
        self._transport.close()

    def data_received(self, byte_data):
        string_data = byte_data.decode()
        self.logger.info('Data received: {}'.format(string_data))
        self._obj_streamer.consume(string_data)

    def on_object_stream_start(self):
        raise RuntimeError('Incorrect JSON Streaming Format: expect a JSON Array to start at root, got object')

    def on_object_stream_end(self):
        del self._obj_streamer
        raise RuntimeError('Incorrect JSON Streaming Format: expect a JSON Array to end at root, got object')

    def on_array_stream_start(self):
        self.logger.info('Array Stream started')

    def on_array_stream_end(self):
        del self._obj_streamer
        self.logger.info('Array Stream ended')

    def on_pair(self, pair):
        self.logger.info('Pair {}'.format(pair))
        raise RuntimeError('Received a key-value pair object - expected elements only')


class VykedProtocol(JSONProtocol):
    def __init__(self, handler):
        super().__init__()
        self._handler = handler

    def connection_made(self, transport):
        peer_name = transport.get_extra_info('peername')
        self.logger.info('Connection from {}'.format(peer_name))
        super(VykedProtocol, self).connection_made(transport)

    def connection_lost(self, exc):
        super(VykedProtocol, self).connection_lost(exc)

    def on_element(self, element):
        super().on_element(element)
        self._handler.receive(packet=element, protocol=self)