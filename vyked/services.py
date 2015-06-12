from asyncio import Future, get_event_loop, coroutine, iscoroutinefunction
from functools import wraps
import json

from again.utils import unique_hex
from aiohttp.web import Response

from .utils.ordered_class_member import OrderedClassMembers

# Service Client decorators


def make_request(func, self, args, kwargs, method):
    params = func(self, *args, **kwargs)
    entity = params.pop('entity', None)
    app_name = params.pop('app_name', None)
    self = params.pop('self')
    response = yield from self._send_http_request(app_name, method, entity, params)
    return response


def get_decorated_fun(method, path, required_params):
    def decorator(func):
        @wraps(func)
        def f(self, *args, **kwargs):
            if isinstance(self, HTTPServiceClient):
                return (yield from make_request(func, self, args, kwargs, method))
            elif isinstance(self, HTTPApplicationService):
                if required_params is not None:
                    req = args[0]
                    query_params = req.GET
                    params = required_params
                    if not isinstance(required_params, list):
                        params = [required_params]
                    if any(map(lambda x: x not in query_params, params)):
                        return Response(status=400, content_type='application/json',
                                        body=json.dumps({'error': 'Required params not found'}).encode())
                wrapped_func = func
                if not iscoroutinefunction(func):
                    wrapped_func = coroutine(func)
                return (yield from wrapped_func(self, *args, **kwargs))

        f.is_http_method = True
        f.method = method
        f.paths = path
        if not isinstance(path, list):
            f.paths = [path]
        return f

    return decorator


def get(path=None, required_params=None):
    return get_decorated_fun('get', path, required_params)


def head(path=None, required_params=None):
    return get_decorated_fun('head', path, required_params)


def options(path=None, required_params=None):
    return get_decorated_fun('options', path, required_params)


def patch(path=None, required_params=None):
    return get_decorated_fun('patch', path, required_params)


def post(path=None, required_params=None):
    return get_decorated_fun('post', path, required_params)


def put(path=None, required_params=None):
    return get_decorated_fun('put', path, required_params)


def trace(path=None, required_params=None):
    return get_decorated_fun('put', path, required_params)


def delete(path=None, required_params=None):
    return get_decorated_fun('delete', path, required_params)


def subscribe(func):
    """
    use to listen for publications from a specific endpoint of a service,
    this method receives a publication from a remote service
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        coroutine_func = func
        if not iscoroutinefunction(func):
            coroutine_func = coroutine(func)
        return (yield from coroutine_func(*args, **kwargs))

    wrapper.is_subscribe = True
    return wrapper


def request(func):
    """
    use to request an api call from a specific endpoint
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        params = func(self, *args, **kwargs)
        self = params.pop('self', None)
        entity = params.pop('entity', None)
        app_name = params.pop('app_name', None)
        request_id = unique_hex()
        params['request_id'] = request_id
        future = self._send_request(app_name, endpoint=func.__name__, entity=entity, params=params)
        return future

    wrapper.is_request = True
    return wrapper


def message_sub(func):
    """
    use to listen for publications from a specific endpoint of a service,
    this method receives a publication from a remote service
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        params = func(self, *args, **kwargs)
        entity = params.pop('entity')
        try:
            app_name = params.pop('app_name')
        except KeyError:
            raise RuntimeError('App name must be specified')
        self._send_message_sub(app_name, endpoint=func.__name__, entity=entity)

    wrapper.is_directed_subscribe = True
    return wrapper


# Service Host Decorators

def publish(func):
    """
    publish the return value of this function as a message from this endpoint
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):  # outgoing
        payload = func(self, *args, **kwargs)
        payload.pop('self', None)
        self._publish(func.__name__, payload)
        return None

    wrapper.is_publish = True

    return wrapper


def message_pub(func):
    """
    publish the return value of this function as a message from this endpoint
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):  # outgoing
        payload = func(self, *args, **kwargs)
        entity = payload.pop('entity')
        self._message(func.__name__, payload, entity)
        return None

    wrapper.is_publish = True

    return wrapper


def api(func):  # incoming
    """
    provide a request/response api
    receives any requests here and return value is the response
    all functions must have the following signature
        - request_id
        - entity (partition/routing key)
        followed by kwargs
    """

    @coroutine
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        rid = kwargs.pop('request_id')
        entity = kwargs.pop('entity')
        from_id = kwargs.pop('from_id')
        wrapped_func = func
        result = None
        error = None
        if not iscoroutinefunction(func):
            wrapped_func = coroutine(func)
        try:
            if len(kwargs):
                result = yield from wrapped_func(self, **kwargs)
            else:
                result = yield from wrapped_func()
        except BaseException as e:
            error = str(e)

        return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result, error=error)

    wrapper.is_api = True
    return wrapper


class _Service:
    _PUB_PKT_STR = 'publish'
    _REQ_PKT_STR = 'request'
    _RES_PKT_STR = 'response'
    _MSG_PUB_PKT_STR = 'message_pub'
    _MSG_SUB_PKT_STR = 'message_sub'

    def __init__(self, service_name, service_version):
        self._service_name = service_name
        self._service_version = service_version
        self._bus = None

    @property
    def name(self):
        return self._service_name

    @property
    def version(self):
        return self._service_version

    @property
    def properties(self):
        return self.name, self.version

    @property
    def bus(self):
        return self._bus

    @bus.setter
    def bus(self, bus):
        self._bus = bus

    @staticmethod
    def time_future(future:Future, timeout:int):
        def timer_callback(f):
            if not f.done() and not f.cancelled():
                f.set_exception(TimeoutError())

        get_event_loop().call_later(timeout, timer_callback, future)


class TCPServiceClient(_Service):
    REQUEST_TIMEOUT_SECS = 10

    def __init__(self, service_name, service_version):
        super(TCPServiceClient, self).__init__(service_name, service_version)
        self._pending_requests = {}

    def _send_request(self, app_name, endpoint, entity, params):
        packet = self._make_packet(app_name, _Service._REQ_PKT_STR, endpoint, params, entity)
        future = Future()
        request_id = params['request_id']
        self._pending_requests[request_id] = future
        self._bus.send(packet)
        _Service.time_future(future, TCPServiceClient.REQUEST_TIMEOUT_SECS)
        return future

    def _send_message_sub(self, app_name, endpoint, entity):
        packet = self._make_packet(app_name, _Service._MSG_SUB_PKT_STR, endpoint, None, entity)
        self._bus.send(packet)

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
            future.set_result(payload['result'])
        elif has_error:
            exception = RequestException()
            exception.error = payload['error']
            future.set_exception(exception)
        else:
            print('Invalid response to request:', packet)

    def _process_publication(self, packet):
        endpoint = packet['endpoint']
        func = getattr(self, endpoint)
        func(**packet['payload'])

    def _make_packet(self, app_name, packet_type, endpoint, params, entity):
        packet = {'pid': unique_hex(),
                  'app': app_name,
                  'service': self.name,
                  'version': self.version,
                  'entity': entity,
                  'endpoint': endpoint,
                  'type': packet_type,
                  'payload': params}
        return packet


class _ServiceHost(_Service):
    def __init__(self, service_name, service_version, host_ip, host_port):
        super(_ServiceHost, self).__init__(service_name, service_version)
        self._ip = host_ip
        self._port = host_port
        self._ronin = False

    def is_for_me(self, service, version):
        return service == self.name and int(version) == self.version

    @property
    def socket_address(self):
        return self._ip, self._port

    @property
    def ronin(self):
        return self._ronin

    @ronin.setter
    def ronin(self, value:bool):
        self._ronin = value


class _TCPServiceHost(_ServiceHost):
    def __init__(self, service_name, service_version, host_ip, host_port):
        # TODO: to be multi-tenant make app_name a list
        super(_TCPServiceHost, self).__init__(service_name, service_version, host_ip, host_port)

    def _publish(self, publication_name, payload):
        packet = self._make_publish_packet(_Service._PUB_PKT_STR, publication_name, payload)
        self._bus.send(packet)

    def _message(self, message_name, payload, entity):
        packet = self._make_publish_packet(_Service._MSG_PUB_PKT_STR, message_name, payload)
        packet['entity'] = entity
        self._bus.send(packet)

    def _make_response_packet(self, request_id: str, from_id: str, entity: str, result: object, error: object):
        if error:
            payload = {'request_id': request_id, 'error': error}
        else:
            payload = {'request_id': request_id, 'result': result}

        packet = {'pid': unique_hex(),
                  'to': from_id,
                  'entity': entity,
                  'type': _Service._RES_PKT_STR,
                  'payload': payload}
        return packet

    def _make_publish_packet(self, packet_type: str, publication_name: str, payload: dict):
        packet = {'service': self.name,
                  'version': self.version,
                  'endpoint': publication_name,
                  'type': packet_type,
                  'payload': payload}
        return packet


class RequestException(Exception):
    pass


class _HTTPServiceHost(_ServiceHost, metaclass=OrderedClassMembers):
    def __init__(self, service_name, service_version, host_ip, host_port, ssl_context=None, allow_cross_domain=False):
        super(_HTTPServiceHost, self).__init__(service_name, service_version, host_ip, host_port)
        self._ssl_context = ssl_context
        self._allow_cross_domain = allow_cross_domain

    @property
    def ssl_context(self):
        return self._ssl_context

    @property
    def cross_domain_allowed(self):
        return self._allow_cross_domain

    def pong(self, request):
        return Response()


class TCPApplicationService(_TCPServiceHost):
    pass


class TCPDomainService(_TCPServiceHost):
    pass


class TCPInfraService(_TCPServiceHost):
    pass


class HTTPApplicationService(_HTTPServiceHost):
    pass


class HTTPDomainService(_HTTPServiceHost):
    pass


class HTTPInfraService(_HTTPServiceHost):
    pass


class HTTPServiceClient(_Service):
    def __init__(self, service_name, service_version):
        super(HTTPServiceClient, self).__init__(service_name, service_version)

    def _send_http_request(self, app_name, method, entity, params):
        response = yield from self._bus.send_http_request(app_name, self.name, self.version, method, entity,
                                                          params)
        return response
