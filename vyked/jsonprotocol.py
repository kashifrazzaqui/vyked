import asyncio
import json
import logging

from jsonstreamer import ObjectStreamer

from .registry import Registry
from .registryclient import RegistryClient
from .services import TCPServiceClient


class JSONProtocol(asyncio.Protocol):

    logger = logging.getLogger(__name__)

    def __init__(self):
        self._pending_data = []
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
        self.logger.debug('Peer closed the connection')

    def send(self, packet: dict):
        string = json.dumps(packet) + ','
        if self._connected:
            self._transport.write(string.encode())
            self.logger.debug('Data sent: {}'.format(string))
        else:
            self._pending_data.append(packet)
            self.logger.debug('Appended data: {}'.format(self._pending_data))

    def close(self):
        self._transport.write('bye]'.encode())  # end the json array
        self._transport.close()

    def data_received(self, byte_data):
        string_data = byte_data.decode()
        self.logger.debug('Data received: {}'.format(string_data))
        self._obj_streamer.consume(string_data)

    def on_object_stream_start(self):
        raise RuntimeError('Incorrect JSON Streaming Format: expect a JSON Array to start at root, got object')

    def on_object_stream_end(self):
        del self._obj_streamer
        raise RuntimeError('Incorrect JSON Streaming Format: expect a JSON Array to end at root, got object')

    def on_array_stream_start(self):
        self.logger.debug('Array Stream started')

    def on_array_stream_end(self):
        del self._obj_streamer
        self.logger.debug('Array Stream ended')

    def on_pair(self, pair):
        self.logger.debug('Pair {}'.format(pair))
        raise RuntimeError('Received a key-value pair object - expected elements only')


class ServiceHostProtocol(JSONProtocol):
    def __init__(self, bus):
        super(ServiceHostProtocol, self).__init__()
        self._bus = bus

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self.logger.debug('Client Connection from {}'.format(peername))
        super(ServiceHostProtocol, self).connection_made(transport)

    def connection_lost(self, exc):
        super(ServiceHostProtocol, self).connection_lost(exc)
        # TODO: think about what needs to be done here

    def on_element(self, element):
        self._bus.host_receive(packet=element, protocol=self)


class ServiceClientProtocol(JSONProtocol):
    def __init__(self, bus):
        super(ServiceClientProtocol, self).__init__()
        self._bus = bus

    def set_service_client(self, service_client:TCPServiceClient):
        self._service_client = service_client

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self.logger.debug('Connected to server{}'.format(peername))
        super(ServiceClientProtocol, self).connection_made(transport)

    def on_element(self, element):
        self._bus.client_receive(packet=element, service_client=self._service_client)


class RegistryProtocol(JSONProtocol):
    def __init__(self, registry:Registry):
        super(RegistryProtocol, self).__init__()
        self._registry = registry

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self._transport = transport
        self.logger.debug('Connected from {}'.format(peername))
        super(RegistryProtocol, self).connection_made(transport)
        # TODO: pass protocol to registry

    def on_element(self, element):
        self._registry.receive(packet=element, registry_protocol=self, transport=self._transport)


class RegistryClientProtocol(JSONProtocol):
    def __init__(self, registry_client:RegistryClient):
        super(RegistryClientProtocol, self).__init__()
        self._registry_client = registry_client

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        self.logger.debug('Connected to server{}'.format(peername))
        super(RegistryClientProtocol, self).connection_made(transport)

    def on_element(self, element):
        self._registry_client.receive(packet=element, registry_protocol=self)
