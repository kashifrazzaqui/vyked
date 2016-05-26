import asyncio
from collections import defaultdict
from functools import partial
import json
import logging
import random

from again.utils import unique_hex
import aiohttp
from retrial.retrial import retry
from random import shuffle

from .services import TCPServiceClient, HTTPServiceClient
from .pubsub import PubSub
from .packet import ControlPacket
from .protocol_factory import get_vyked_protocol
from .utils.jsonencoder import VykedEncoder
from .exceptions import ClientNotFoundError
from .utils.client_stats import ClientStats

HTTP = 'http'
TCP = 'tcp'


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
    def __init__(self, registry_client):
        registry_client.conn_handler = self
        self._registry_client = registry_client
        self._client_protocols = {}
        self._pingers = {}
        self._node_clients = {}
        self._service_clients = []
        self.tcp_host = None
        self.http_host = None
        self._host_id = unique_hex()
        self._ronin = False
        self._registered = False
        self._logger = logging.getLogger(__name__)

    def _create_service_clients(self):
        futures = []
        for sc in self._service_clients:
            for host, port, node_id, service_type in self._registry_client.get_all_addresses(*sc.properties):
                if service_type == 'tcp':
                    self._node_clients[node_id] = sc
                    future = self._connect_to_client(host, node_id, port, service_type, sc)
                    futures.append(future)
        return asyncio.gather(*futures, return_exceptions=False)

    def register(self):
        clients = self.tcp_host.clients if self.tcp_host else self.http_host.clients
        for client in clients:
            if isinstance(client, (TCPServiceClient, HTTPServiceClient)):
                client.bus = self
        self._service_clients = clients
        asyncio.get_event_loop().run_until_complete(self._registry_client.connect())

    def registration_complete(self):
        if not self._registered:
            self._create_service_clients()
            self._registered = True

    def new_instance(self, service, version, host, port, node_id, type):
        sc = next(sc for sc in self._service_clients if sc.name == service and sc.version == version)
        if type == 'tcp':
            self._node_clients[node_id] = sc
            asyncio.async(self._connect_to_client(host, node_id, port, type, sc))

    def send(self, packet: dict):
        packet['from'] = self._host_id
        func = getattr(self, '_' + packet['type'] + '_sender')
        func(packet)

    def _request_sender(self, packet: dict):
        """
        Sends a request to a server from a ServiceClient
        auto dispatch method called from self.send()
        """
        node = self._get_node_id_for_packet(packet)
        node_id = node[2]
        client_protocol = self._client_protocols.get(node_id)[0]
        client_host = self._client_protocols.get(node_id)[1]

        if node_id and client_protocol:
            if client_protocol.is_connected():
                packet['to'] = node_id
                client_protocol.send(packet)
                ClientStats.update(packet['service'], client_host)

            else:
                self._client_protocols.pop(node_id)
                self.new_instance(packet['service'], packet['version'], *node)
                self._request_sender(packet)
        else:
            # No node found to send request
            if node_id:
                self._request_sender(packet)
            else:
                self._logger.error('Out of %s, Client Not found for packet %s', self._client_protocols.keys(), packet)
                raise ClientNotFoundError()

    @retry(should_retry_for_exception=_retry_for_exception, strategy=[0, 2, 4, 8, 16, 32], max_attempts=6)
    @asyncio.coroutine
    def _connect_to_client(self, host, node_id, port, service_type, service_client):

        _, protocol = yield from asyncio.get_event_loop().create_connection(partial(get_vyked_protocol, service_client),
                                                                            host, port, ssl=service_client._ssl_context)
        self._client_protocols[node_id] = (protocol, host)

    @staticmethod
    def _create_json_service_name(app, service, version):
        return {'app': app, 'service': service, 'version': version}

    @staticmethod
    def _handle_ping(packet, protocol):
        protocol.send(ControlPacket.pong(packet['node_id']))

    def _handle_pong(self, node_id, count):
        pinger = self._pingers[node_id]
        asyncio.async(pinger.pong_received(count))

    def _get_node_id_for_packet(self, packet):
        service, version, entity = packet['service'], packet['version'], packet['entity']
        node = self._registry_client.resolve(service, version, entity, TCP)
        return node

    def handle_ping_timeout(self, node_id):
        self._logger.info("Service client connection timed out {}".format(node_id))
        self._pingers.pop(node_id, None)
        service_props = self._registry_client.get_for_node(node_id)
        self._logger.info('service client props {}'.format(service_props))
        if service_props is not None:
            host, port, _node_id, _type = service_props
            asyncio.async(self._connect_to_client(host, _node_id, port, _type))

    def receive(self, packet: dict, protocol, transport):
        if packet['type'] == 'ping':
            self._handle_ping(packet, protocol)
        elif packet['type'] == 'pong':
            self._handle_pong(packet['node_id'], packet['count'])
        elif packet['type'] == 'change_log_level':
            self._handle_log_change(packet, protocol)
        elif packet['type'] == 'get_tasks':
            protocol.send(len(list(asyncio.Task.all_tasks())))
        else:
            if self.tcp_host.is_for_me(packet['service'], packet['version']):
                func = getattr(self, '_' + packet['type'] + '_receiver')
                func(packet, protocol)
            else:
                self._logger.warn('wrongly routed packet: ', packet)

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

    def handle_connected(self):
        if self.tcp_host:
            self._registry_client.register(self.tcp_host.host, self.tcp_host.port, self.tcp_host.name,
                                           self.tcp_host.version, self.tcp_host.node_id, self.tcp_host.clients, 'tcp')
        if self.http_host:
            self._registry_client.register(self.http_host.host, self.http_host.port, self.http_host.name,
                                           self.http_host.version, self.http_host.node_id, self.http_host.clients,
                                           'http')

    def _handle_log_change(self, packet, protocol):
        try:
            level = getattr(logging, packet['level'].upper())
        except KeyError as e:
            self._logger.error(e)
            protocol.send('Malformed packet')
            return
        except AttributeError as e:
            self._logger.error(e)
            protocol.send('Allowed logging levels: DEBUG, INFO, WARNING, ERROR, CRITICAL')
            return
        logging.getLogger().setLevel(level)
        for handler in logging.getLogger().handlers:
            handler.setLevel(level)
        protocol.send('Logging level updated')


class PubSubBus:

    def __init__(self, registry_client, ssl_context=None):
        self._pubsub_handler = None
        self._registry_client = registry_client
        self._clients = None
        self._ssl_context = ssl_context

    def create_pubsub_handler(self, host, port):
        self._pubsub_handler = PubSub(host, port)
        yield from self._pubsub_handler.connect()

    def register_for_subscription(self, host, port, node_id, clients):
        self._clients = clients
        subs_list = []
        xsubs_list4registry = []
        xsubs_list4redis = []
        for client in filter(lambda x: isinstance(x, TCPServiceClient), clients):
            for each in dir(client):
                fn = getattr(client, each)
                if getattr(fn, 'is_subscribe', False):
                    subs_list.append(self._get_pubsub_key(client.name, client.version, fn.__name__))
                elif getattr(fn, 'is_xsubscribe', False):
                    xsubs_list4registry.append((client.name, client.version, fn.__name__, getattr(fn, 'strategy')))
                    xsubs_list4redis.append(self._get_pubsub_key(client.name, client.version, fn.__name__,
                                                                 node_id=node_id))
        self._registry_client.x_subscribe(host, port, node_id, xsubs_list4registry)
        yield from self._pubsub_handler.subscribe(subs_list + xsubs_list4redis, handler=self.subscription_handler)

    def publish(self, service, version, endpoint, payload):
        endpoint_key = self._get_pubsub_key(service, version, endpoint)
        asyncio.async(self._pubsub_handler.publish(endpoint_key, json.dumps(payload, cls=VykedEncoder)))
        asyncio.async(self.xpublish(service, version, endpoint, payload))

    def xpublish(self, service, version, endpoint, payload):
        subscribers = yield from self._registry_client.get_subscribers(service, version, endpoint)
        strategies = defaultdict(list)
        for subscriber in subscribers:
            strategies[(subscriber['service'], subscriber['version'])].append(
                (subscriber['host'], subscriber['port'], subscriber['node_id'], subscriber['strategy']))
        for key, value in strategies.items():
            if value[0][3] == 'LEADER':
                node_id = value[0][2]
            else:
                random_metadata = random.choice(value)
                node_id = random_metadata[2]
            node_ids = [subscriber[2] for subscriber in value]
            shuffle(node_ids)
            node_ids.insert(0, node_ids.pop(node_ids.index(node_id)))
            asyncio.async(self.retry_xpublish(payload, service, version, endpoint, node_ids))

    def retry_xpublish(self, payload, service, version, endpoint, node_ids):
        for node_id in node_ids:
            if (yield from self.publish_to_redis(payload, service, version, endpoint, node_id))==1:
                break

    @asyncio.coroutine
    def publish_to_redis(self, payload, service, version, endpoint, node_id):
        endpoint_key = self._get_pubsub_key(service, version, endpoint, node_id=node_id)
        result = yield from self._pubsub_handler.publish(endpoint_key, json.dumps(payload, cls=VykedEncoder))
        return result

    def subscription_handler(self, endpoint, payload):
        elements = endpoint.split('/')
        node_id = None
        if len(elements) > 3:
            service, version, endpoint, node_id = elements
        else:
            service, version, endpoint = elements
        client = [sc for sc in self._clients if (sc.name == service and sc.version == version)][0]
        func = getattr(client, endpoint)
        if node_id:
            asyncio.async(func(json.loads(payload)))
        else:
            asyncio.async(func(**json.loads(payload)))

    @staticmethod
    def _get_pubsub_key(service, version, endpoint, node_id=None):
        if node_id:
            return '/'.join((service, str(version), endpoint, node_id))
        return '/'.join((service, str(version), endpoint))
