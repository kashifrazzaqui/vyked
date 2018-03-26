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
from .packet import ControlPacket, MessagePacket
from .protocol_factory import get_vyked_protocol
from .utils.jsonencoder import VykedEncoder
from .exceptions import ClientNotFoundError, RecursionDepthExceeded, RequestException


HTTP = 'http'
TCP = 'tcp'
MAX_RETRY_COUNT = 5


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
        self.pubsub = None
        self._logger = logging.getLogger(__name__)
        self._aiohttp_session = aiohttp.ClientSession()

    def _create_service_clients(self):
        futures = []
        for sc in self._service_clients:
            for host, port, node_id, service_type in self._registry_client.get_all_addresses(*sc.properties):
                if service_type == 'tcp' and node_id not in self._node_clients.keys():
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
        if type == 'tcp' and node_id not in self._node_clients.keys():
            self._node_clients[node_id] = sc
            asyncio.async(self._connect_to_client(host, node_id, port, type, sc))

    def send(self, packet: dict):
        packet['from'] = self._host_id
        func = getattr(self, '_' + packet['type'] + '_sender')
        func(packet)

    def _request_sender(self, packet: dict, retry_count=0):
        """
        Sends a request to a server from a ServiceClient
        auto dispatch method called from self.send()
        """
        if retry_count == MAX_RETRY_COUNT:
            _msg = 'could not connect to service: {} while calling endpoint: {}'.format(packet['service'],
                                                                                        packet['endpoint'])
            raise RecursionDepthExceeded(_msg)

        node = self._get_node_id_for_packet(packet)
        node_id = node[2]
        try:
            client_protocol = self._client_protocols.get(node_id)
        except TypeError:
            client_protocol = None

        if node_id and client_protocol:
            if client_protocol.is_connected():
                packet['to'] = node_id
                client_protocol.send(packet)

            else:
                self._client_protocols.pop(node_id)
                self.new_instance(packet['service'], packet['version'], *node)
                self._request_sender(packet, retry_count)
        else:
            # No node found to send request
            if node_id:
                retry_count += 1
                self._request_sender(packet, retry_count)
            else:
                self._logger.error('Out of %s, Client Not found for packet %s', self._client_protocols.keys(), packet)
                raise ClientNotFoundError()


    def _send_http_request(self, app: str, service: str, version: str, method: str, entity: str, params: dict):
            """
            A convenience method that allows you to send a well formatted http request to another service
            """
            host, port, node_id, service_type = self._registry_client.resolve(service, version, entity, HTTP)
            path = params.pop('path')
            url = 'http://{}:{}{}'.format(host, port, path)
            http_keys = ['data', 'headers', 'cookies', 'auth', 'allow_redirects', 'compress', 'chunked']
            request_packet = None
            if method =='post':
                data = params.pop('data', {})
                request_packet = MessagePacket.request(name = service, version = version, app_name = app, packet_type= 'request', endpoint = path, params= data, entity = entity)
                params['data'] = json.dumps(request_packet, cls=VykedEncoder)
            kwargs = {k: params[k] for k in http_keys if k in params}
            query_params = params.pop('params', {})
            if app is not None:
                query_params['app'] = app
            query_params['version'] = version
            query_params['service'] = service
            self._logger.debug("TCP  TO HTTP CALL  FOR  {}, HOST {}, PORT {}, PARAMS {}".format(path,host, port, params))
            response = None
            try:
                response = yield from self._aiohttp_session.request(method, url, params=query_params, **kwargs)
                result =   yield from response.json()
            except Exception as e:
                exception = RequestException()
                if response:
                    exception.error = "{}_{}".format(response.status, e)
                else:
                    exception.error = "{}_{}".format(503, "Connection to  http server failed")
                raise exception
            self._logger.debug("TCP  TO HTTP RESPONSE {}".format(result))
            if (not result) or ( result.get('payload',None) == None):
                raise ClientNotFoundError("{}_{}".format(404, "No response from client"))
            elif 'error' in result['payload']:
                exception = RequestException()
                exception.error = result['payload']['error']
                raise exception
            return result['payload']['result']

    @retry(should_retry_for_exception=_retry_for_exception, strategy=[0, 2, 4, 8, 16, 32], max_attempts=6)
    @asyncio.coroutine
    def _connect_to_client(self, host, node_id, port, service_type, service_client):

        _, protocol = yield from asyncio.get_event_loop().create_connection(partial(get_vyked_protocol, service_client),
                                                                            host, port, ssl=service_client._ssl_context)
        self._client_protocols[node_id] = protocol

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
        elif packet['type'] == 'get_queues':
            protocol.send(str([(x._service_name, len(x._pending_requests.keys())) for x in self._service_clients]))
        elif packet['type'] == 'blacklist':
            self._handle_blacklist(protocol)
        else:
            if self.tcp_host.is_for_me(packet['service'], packet['version']):
                func = getattr(self, '_' + packet['type'] + '_receiver')
                func(packet, protocol)
            else:
                self._logger.warn('wrongly routed packet: ', packet)

    def _request_receiver(self, packet, protocol):
        api_fn = getattr(self.tcp_host, packet['endpoint'])
        if getattr(api_fn,'is_api', None) and api_fn.is_api:
            from_node_id = packet['from']
            entity = packet['entity']
            future = asyncio.async(api_fn(from_id=from_node_id, entity=entity, **packet['payload']))

            def send_result(f):
                result_packet = f.result()
                protocol.send(result_packet)

            future.add_done_callback(send_result)
        else:
            self._logger.error('no api found for packet: ', packet)

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

    def _handle_blacklist(self, protocol):
        """ """
        if self.tcp_host:
            self._registry_client.blacklist_service(self.tcp_host.host, self.tcp_host.port)
        if self.http_host:
            self._registry_client.blacklist_service(self.http_host.host, self.http_host.port)
        self.pubsub._is_blacklisted = True
        protocol.send('Service Blacklisted Successfully')


class PubSubBus:

    def __init__(self, registry_client, ssl_context=None):
        self._pubsub_handler = None
        self._registry_client = registry_client
        self._clients = None
        self._ssl_context = ssl_context

    def create_pubsub_handler(self, host, port):
        self._pubsub_handler = PubSub(host, port)
        yield from self._pubsub_handler.connect()
        return self._pubsub_handler

    def register_for_subscription(self, host, port, node_id, clients, service):
        self._clients = clients
        self._service = service
        subs_list = []
        xsubs_list4registry = []
        xsubs_list4redis = []
        for client in filter(lambda x: isinstance(x, TCPServiceClient), clients):
            client._pubsub_bus = self
            for each in dir(client):
                fn = getattr(client, each)
                if getattr(fn, 'is_subscribe', False):
                    subs_list.append(self._get_pubsub_key(client.name, client.version, fn.__name__))
                elif getattr(fn, 'is_xsubscribe', False):
                    xsubs_list4registry.append((client.name, client.version, fn.__name__, getattr(fn, 'strategy')))
                    xsubs_list4redis.append('/'.join((client.name, client.version, fn.__name__, service.name, service.version)))
        asyncio.async(self.message_queue_popper(xsubs_list4redis))
        self._registry_client.x_subscribe(host, port, node_id, xsubs_list4registry)
        yield from self._pubsub_handler.subscribe(subs_list, handler=self.subscription_handler)

    def publish(self, service, version, endpoint, payload):
        endpoint_key = self._get_pubsub_key(service, version, endpoint)
        asyncio.async(self._pubsub_handler.publish(endpoint_key, json.dumps(payload, cls=VykedEncoder)))
        asyncio.async(self.xpublish(service, version, endpoint, payload))

    def xpublish(self, service, version, endpoint, payload):
        subscribers = yield from self._registry_client.get_subscribers(service, version, endpoint)
        strategies = []
        for subscriber in subscribers:
            strategies.append((subscriber['service'], subscriber['version']))
        for element in strategies:
            asyncio.async(self._pubsub_handler.add_to_queue('/'.join((service, version, endpoint, element[0], element[1])), 
                json.dumps(payload, cls=VykedEncoder)))

    @asyncio.coroutine
    def publish_to_redis(self, payload, service, version, endpoint, node_id):
        endpoint_key = self._get_pubsub_key(service, version, endpoint, node_id=node_id)
        result = yield from self._pubsub_handler.publish(endpoint_key, json.dumps(payload, cls=VykedEncoder))
        return result

    def enqueue(self, endpoint, payload):
        asyncio.async(self._pubsub_handler.add_to_queue(str(endpoint), json.dumps(payload, cls=VykedEncoder)))

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

    def task_queue_handler(self, queue_name, payload, blocking=False):
        if '/' in queue_name:
            service, version, endpoint, _, _ = queue_name.split('/')
            client = [sc for sc in self._clients if (sc.name == service and sc.version == version)][0]
            func = getattr(client, endpoint, None)
        else:
            for each in dir(self._service):
                fn = getattr(self._service, each)
                if getattr(fn, 'queue_name', '') == queue_name and getattr(fn, 'is_task_queue', False):
                    func = fn
                    break
        if func:
            if blocking:
                yield from func(**json.loads(payload))
            elif getattr(func, 'blocking', False):
                yield from func(json.loads(payload))
            else:
                asyncio.async(func(json.loads(payload)))

    def message_queue_popper(self, endpoints):
        if endpoints:
            yield from self._pubsub_handler.task_getter(endpoints, self.task_queue_handler)

    def register_for_task_queues(self, service):
        endpoints = []
        for each in dir(service):
            fn = getattr(service, each)
            if getattr(fn, 'is_task_queue', False):
                if fn.queue_name not in endpoints:
                    endpoints.append(fn.queue_name)
        if len(endpoints):
            yield from self._pubsub_handler.task_getter(endpoints, self.task_queue_handler, blocking=True)
