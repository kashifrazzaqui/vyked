import asyncio
from jsonstreamer import ObjectStreamer
import json

class StreamingJSONProtocol(asyncio.Protocol):
    def __init__(self, bus):
        self._pending_data = []
        self._bus = bus
        self._connected = False
        self._transport = None
        self._obj_streamer = None

    def _make_frame(self, packet):
        string = json.dumps(packet) + ','
        return string

    def _write_pending_data(self):
        for packet in self._pending_data:
            frame = self._make_frame(packet)
            self._transport.write(frame.encode())

    def connection_made(self, transport):
        self._connected = True
        self._transport = transport
        self._obj_streamer = ObjectStreamer()
        self._obj_streamer.auto_listen(self, prefix='on_')

        self._transport.write('['.encode())  # start a json array
        self._write_pending_data()

    def connection_lost(self, exc):
        self._connected = False
        print('Peer closed the connection')

    def send(self, packet:'dict'):
        string = json.dumps(packet)
        if self._connected:
            self._transport.write(string.encode())
        else:
            self._pending_data.append(packet)
            print('Appended data: {}'.format(self._pending_data))

    def close(self):
        self._transport.write('bye]'.encode())  # end the json array
        self._transport.close()

    def data_received(self, byte_data):
        string_data = byte_data.decode()
        self._obj_streamer.consume(string_data)

    def on_object_stream_start(self):
        raise RuntimeError('Incorrect JSON Streaming Format: expect a JSON Array to start at root, got object')

    def on_object_stream_end(self):
        del self._obj_streamer
        raise RuntimeError('Incorrect JSON Streaming Format: expect a JSON Array to end at root, got object')

    def on_array_stream_start(self):
        print('Array Stream started')

    def on_array_stream_end(self):
        del self._obj_streamer
        print('Array Stream ended')

    def on_pair(self, pair):
        print('Pair {}'.format(pair))
        raise RuntimeError('Received a key-value pair object - expected elements only')

    def on_element(self, element):
        self._bus.receive(element)

class StreamingJSONServerProtocol(StreamingJSONProtocol):
    def __init__(self, bus):
        super(StreamingJSONServerProtocol, self).__init__(bus)

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Client Connection from {}'.format(peername))
        self._bus.register_client(peername, self)
        super(StreamingJSONServerProtocol, self).connection_made(transport)

class StreamingJSONClientProtocol(StreamingJSONProtocol):
    def __init__(self, bus):
        super(StreamingJSONClientProtocol, self).__init__(bus)

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self._bus.register_server(peername, self)
        print('Connected to server{}'.format(peername))
        super(StreamingJSONClientProtocol, self).connection_made(transport)

