import asyncio
from functools import partial
import json
import logging

from again.utils import unique_hex

from retrial.retrial import retry
import aiohttp

from .registry_client import RegistryClient
from .services import TCPServiceClient, HTTPServiceClient
from .pubsub import PubSub
from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol

HTTP = 'http'
TCP = 'tcp'

_logger = logging.getLogger(__name__)


def _retry_for_client_conn(result):
    if isinstance(result, tuple):
        return not isinstance(result[0], asyncio.transports.Transport) or not isinstance(result[1], asyncio.Protocol)
    return True


def _retry_for_pub(result):
    return not result


def _retry_for_exception(_):
    return True


class HTTPBus:
    def __init__(self, registry_client):
        self._registry_client = registry_client

    def send_http_request(self, app: str, service: str, version: str, method: str, entity: str, params: dict):
        """
        A convenience method that allows you to send a well formatted http request to another service
        """
        host, port, node_id, service_type = self._registry_client.resolve(service, version, entity, HTTP)

        url = 'http://{}:{}{}'.format(host, port, params.pop('path'))

        http_keys = ['data', 'headers', 'cookies', 'auth', 'allow_redirects', 'compress', 'chunked']
        kwargs = {k: params[k] for k in http_keys if k in params}

        query_params = params.pop('params', {})

        if app is not None:
            query_params['app'] = app

        query_params['version'] = version
        query_params['service'] = service

        response = yield from aiohttp.request(method, url, params=query_params, **kwargs)
        return response


class TCPBus:
    def __init__(self):
        self._registry_client = None
        self._client_protocols = {}
        self._pingers = {}
        self._node_clients = {}
        self._service_clients = []
        self._pending_requests = []
        self.tcp_host = None
        self.http_host = None
        self._host_id = unique_hex()
        self._ronin = False
        self._registered = False

    def _create_service_clients(self):
        futures = []
        for sc in self._service_clients:
            for host, port, node_id, service_type in self._registry_client.get_all_addresses(sc.properties):
                self._node_clients[node_id] = sc
                future = self._connect_to_client(host, node_id, port, service_type, sc)
                futures.append(future)
        return asyncio.gather(*futures, return_exceptions=False)

    def register(self, host, port, service, version, clients, service_type):
        for client in clients:
            if isinstance(client, (TCPServiceClient, HTTPServiceClient)):
                client.bus = self
        self._service_clients = clients
        self._registry_client.register(host, port, service, version, clients, service_type)

    def registration_complete(self):
        if not self._registered:
            f = self._create_service_clients()
            self._registered = True

            def fun(_):
                if self.tcp_host:
                    self._clear_request_queue()

            f.add_done_callback(fun)

    def send(self, packet: dict):
        packet['from'] = self._host_id
        func = getattr(self, '_' + packet['type'] + '_sender')
        func(packet)

    def _request_sender(self, packet: dict):
        """
        Sends a request to a server from a ServiceClient
        auto dispatch method called from self.send()
        """
        self._pending_requests.append(packet)
        self._clear_request_queue()

    @retry(should_retry_for_result=_retry_for_client_conn, should_retry_for_exception=_retry_for_exception, timeout=10,
           strategy=[0, 2, 2, 4])
    def _connect_to_client(self, host, node_id, port, service_type, service_client):
        _logger.info('node_id' + node_id)
        future = asyncio.async(
            asyncio.get_event_loop().create_connection(partial(get_vyked_protocol, service_client), host, port))
        future.add_done_callback(
            partial(self._service_client_connection_callback, self._node_clients[node_id], node_id, service_type))
        return future

    def _service_client_connection_callback(self, sc, node_id, service_type, future):
        _, protocol = future.result()
        # TODO : handle pinging
        # if service_type == TCP:
        #     pinger = Pinger(self, asyncio.get_event_loop())
        #     self._pingers[node_id] = pinger
        #     pinger.register_tcp_service(protocol, node_id)
        #     asyncio.async(pinger.start_ping())
        self._client_protocols[node_id] = protocol

    def setup_registry_client(self, host: str, port: int):
        self._registry_client = RegistryClient(asyncio.get_event_loop(), host, port, self)
        self._registry_client.connect()

    @staticmethod
    def _create_json_service_name(app, service, version):
        return {'app': app, 'service': service, 'version': version}

    @staticmethod
    def _handle_ping(packet, protocol):
        protocol.send(ControlPacket.pong(packet['node_id'], packet['count']))

    def _handle_pong(self, node_id, count):
        pinger = self._pingers[node_id]
        asyncio.async(pinger.pong_received(count))

    def _clear_request_queue(self):
        self._pending_requests[:] = [each for each in self._pending_requests if not self._send_packet(each)]

    def _send_packet(self, packet):
        node_id = self._get_node_id_for_packet(packet)
        if node_id is not None:
            client_protocol = self._client_protocols[node_id]
            if client_protocol.is_connected():
                packet['to'] = node_id
                client_protocol.send(packet)
                return True
            else:
                return False
        return False

    def _get_node_id_for_packet(self, packet):
        app, service, version, entity = packet['app'], packet['service'], packet['version'], packet['entity']
        node = self._registry_client.resolve(service, version, entity, TCP)
        return node[2] if node else None

    def handle_ping_timeout(self, node_id):
        _logger.info("Service client connection timed out {}".format(node_id))
        self._pingers.pop(node_id, None)
        service_props = self._registry_client.get_for_node(node_id)
        _logger.info('service client props {}'.format(service_props))
        if service_props is not None:
            host, port, _node_id, _type = service_props
            asyncio.async(self._connect_to_client(host, _node_id, port, _type))

    def receive(self, packet: dict, protocol, transport):
        if packet['type'] == 'ping':
            self._handle_ping(packet, protocol)
        elif packet['type'] == 'pong':
            self._handle_pong(packet['node_id'], packet['count'])
        else:
            if self.tcp_host.is_for_me(packet['service'], packet['version']):
                func = getattr(self, '_' + packet['type'] + '_receiver')
                func(packet, protocol)
            else:
                _logger.warn('wrongly routed packet: ', packet)

    def _request_receiver(self, packet, protocol):
        api_fn = getattr(self.tcp_host, packet['endpoint'])
        if api_fn.is_api:
            from_node_id = packet['from']
            entity = packet['entity']
            future = asyncio.async(api_fn(from_id=from_node_id, entity=entity, **packet['payload']))

            def send_result(f):
                result_packet = f.result()
                protocol.send(result_packet)

            future.add_done_callback(send_result)
        else:
            print('no api found for packet: ', packet)


class PubSubBus:
    def __init__(self):
        self._pubsub_handler = None
        self._clients = None

    def create_pubsub_handler(self, host, port):
        self._pubsub_handler = PubSub(host, port)
        yield from self._pubsub_handler.connect()

    def register_for_subscription(self, clients):
        self._clients = clients
        subscription_list = []
        for client in clients:
            if isinstance(client, TCPServiceClient):
                for each in dir(client):
                    fn = getattr(client, each)
                    if callable(fn) and getattr(fn, 'is_subscribe', False):
                        subscription_list.append(self._get_pubsub_key(client.name, client.version, fn.__name__))
        yield from self._pubsub_handler.subscribe(subscription_list, handler=self.subscription_handler)

    def publish(self, service, version, endpoint, payload):
        endpoint = self._get_pubsub_key(service, version, endpoint)
        asyncio.async(self._retry_publish(endpoint, payload))

    @retry(should_retry_for_result=_retry_for_pub, should_retry_for_exception=_retry_for_exception, timeout=10,
           strategy=[0, 2, 2, 4])
    def _retry_publish(self, service, version, endpoint, payload):
        return (yield from self._pubsub_handler.publish(service, version, endpoint, payload))

    def subscription_handler(self, service, version, endpoint, payload):
        client = [sc for sc in self._clients if (sc.name == service and sc.version == version)][0]
        func = getattr(client, endpoint)
        asyncio.async(func(**json.loads(payload)))

    @staticmethod
    def _get_pubsub_key(service, version, endpoint):
        return '/'.join((service, str(version), endpoint))
