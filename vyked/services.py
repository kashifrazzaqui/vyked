from asyncio import Future, get_event_loop
import json

from again.utils import unique_hex

from aiohttp.web import Response

from .packet import MessagePacket
from .exceptions import RequestException, ClientException
from .utils.ordered_class_member import OrderedClassMembers
from .utils.stats import Aggregator


class _Service:
    _PUB_PKT_STR = 'publish'
    _REQ_PKT_STR = 'request'
    _RES_PKT_STR = 'response'

    def __init__(self, service_name, service_version):
        self._service_name = service_name.lower()
        self._service_version = str(service_version)
        self._tcp_bus = None
        self._pubsub_bus = None
        self._http_bus = None

    @property
    def name(self):
        return self._service_name

    @property
    def version(self):
        return self._service_version

    @property
    def properties(self):
        return self.name, self.version

    @staticmethod
    def time_future(future: Future, timeout: int):
        def timer_callback(f):
            if not f.done() and not f.cancelled():
                f.set_exception(TimeoutError())

        get_event_loop().call_later(timeout, timer_callback, future)


class TCPServiceClient(_Service):
    REQUEST_TIMEOUT_SECS = 600

    def __init__(self, service_name, service_version, ssl_context=None):
        super(TCPServiceClient, self).__init__(service_name, service_version)
        self._pending_requests = {}
        self.tcp_bus = None
        self._ssl_context = ssl_context

    @property
    def ssl_context(self):
        return self._ssl_context

    def _send_request(self, app_name, endpoint, entity, params):
        packet = MessagePacket.request(self.name, self.version, app_name, _Service._REQ_PKT_STR, endpoint, params,
                                       entity)
        future = Future()
        request_id = params['request_id']
        self._pending_requests[request_id] = future
        try:
            self.tcp_bus.send(packet)
        except ClientException:
            if not future.done() and not future.cancelled():
                error = 'Client not found'
                exception = ClientException(error)
                exception.error = error
                future.set_exception(exception)
        _Service.time_future(future, TCPServiceClient.REQUEST_TIMEOUT_SECS)
        return future

    def receive(self, packet: dict, protocol, transport):
        if packet['type'] == 'ping':
            pass
        else:
            self._process_response(packet)

    def process_packet(self, packet):
        if packet['type'] == _Service._RES_PKT_STR:
            self._process_response(packet)
        elif packet['type'] == _Service._PUB_PKT_STR:
            self._process_publication(packet)
        else:
            print('Invalid packet', packet)

    def _process_response(self, packet):
        payload = packet['payload']
        request_id = payload['request_id']
        has_result = 'result' in payload
        has_error = 'error' in payload
        future = self._pending_requests.pop(request_id)
        if has_result:
            if not future.done() and not future.cancelled():
                future.set_result(payload['result'])
        elif has_error:
            if payload.get('failed', False):
                if not future.done() and not future.cancelled():
                    future.set_exception(Exception(payload['error']))
            else:
                exception = RequestException()
                exception.error = payload['error']
                if not future.done() and not future.cancelled():
                    future.set_exception(exception)
        else:
            print('Invalid response to request:', packet)

    def _process_publication(self, packet):
        endpoint = packet['endpoint']
        func = getattr(self, endpoint)
        func(**packet['payload'])


class _ServiceHost(_Service):
    def __init__(self, service_name, service_version, host_ip, host_port):
        super(_ServiceHost, self).__init__(service_name, service_version)
        self._node_id = unique_hex()
        self._ip = host_ip
        self._port = host_port
        self._clients = []

    def is_for_me(self, service, version):
        return service == self.name and version == self.version

    @property
    def node_id(self):
        return self._node_id

    @property
    def tcp_bus(self):
        return self._tcp_bus

    @tcp_bus.setter
    def tcp_bus(self, bus):
        for client in self._clients:
            if isinstance(client, TCPServiceClient):
                client.tcp_bus = bus
        self._tcp_bus = bus

    @property
    def http_bus(self):
        return self._http_bus

    @http_bus.setter
    def http_bus(self, bus):
        for client in self._clients:
            if isinstance(client, HTTPServiceClient):
                client._http_bus = self._http_bus
        self._http_bus = bus

    @property
    def pubsub_bus(self):
        return self._pubsub_bus

    @pubsub_bus.setter
    def pubsub_bus(self, bus):
        self._pubsub_bus = bus

    @property
    def clients(self):
        return self._clients

    @clients.setter
    def clients(self, clients):
        self._clients = clients

    def register(self):
        self._tcp_bus.register()

    @property
    def socket_address(self):
        return self._ip, self._port

    @property
    def host(self):
        return self._ip

    @property
    def port(self):
        return self._port


class TCPService(_ServiceHost):
    def __init__(self, service_name, service_version, host_ip=None, host_port=None, ssl_context=None):
        super(TCPService, self).__init__(service_name, service_version, host_ip, host_port)
        self._ssl_context = ssl_context

    @property
    def ssl_context(self):
        return self._ssl_context

    def _publish(self, endpoint, payload):
        self._pubsub_bus.publish(self.name, self.version, endpoint, payload)

    def _xpublish(self, endpoint, payload, strategy):
        self._pubsub_bus.xpublish(self.name, self.version, endpoint, payload, strategy)

    @staticmethod
    def _make_response_packet(request_id: str, from_id: str, entity: str, result: object, error: object,
                              failed: bool, old_api=None, replacement_api=None):
        if error:
            payload = {'request_id': request_id, 'error': error, 'failed': failed}
        else:
            payload = {'request_id': request_id, 'result': result}
        if old_api:
            payload['old_api'] = old_api
            if replacement_api:
                payload['replacement_api'] = replacement_api
        packet = {'pid': unique_hex(),
                  'to': from_id,
                  'entity': entity,
                  'type': _Service._RES_PKT_STR,
                  'payload': payload}
        return packet


def default_preflight_response(request):
    headers = {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE',
               'Access-Control-Allow-Headers': 'accept, content-type'}
    return Response(status=200, headers=headers)


class HTTPService(_ServiceHost, metaclass=OrderedClassMembers):
    def __init__(self, service_name, service_version, host_ip=None, host_port=None, ssl_context=None,
                 allow_cross_domain=False,
                 preflight_response=default_preflight_response):
        super(HTTPService, self).__init__(service_name, service_version, host_ip, host_port)
        self._ssl_context = ssl_context
        self._allow_cross_domain = allow_cross_domain
        self._preflight_response = preflight_response

    @property
    def ssl_context(self):
        return self._ssl_context

    @property
    def cross_domain_allowed(self):
        return self._allow_cross_domain

    @property
    def preflight_response(self):
        return self._preflight_response

    @staticmethod
    def pong(_):
        return Response()

    @staticmethod
    def stats(_):
        res_d = Aggregator.dump_stats()
        return Response(status=200, content_type='application/json', body=json.dumps(res_d).encode())


class HTTPServiceClient(_Service):
    def __init__(self, service_name, service_version):
        super(HTTPServiceClient, self).__init__(service_name, service_version)

    def _send_http_request(self, app_name, method, entity, params):
        response = yield from self._http_bus.send_http_request(app_name, self.name, self.version, method, entity,
                                                               params)
        return response
