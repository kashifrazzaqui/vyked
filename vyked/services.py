import json
import logging
import socket
import time
import setproctitle

from asyncio import iscoroutine, coroutine, wait_for, TimeoutError, Future, get_event_loop, async
from functools import wraps, partial
from aiohttp.web import Response
from again.utils import unique_hex

from .packet import MessagePacket
from .exceptions import RequestException, ClientException, VykedServiceException
from .utils.ordered_class_member import OrderedClassMembers
from .utils.stats import Aggregator, Stats


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


def subscribe(func):
    """
    use to listen for publications from a specific endpoint of a service,
    this method receives a publication from a remote service
    """
    wrapper = _get_subscribe_decorator(func)
    wrapper.is_subscribe = True
    return wrapper


def xsubscribe(func=None, strategy='DESIGNATION'):
    """
    Used to listen for publications from a specific endpoint of a service. If multiple instances
    subscribe to an endpoint, only one of them receives the event. And the publish event is retried till
    an acknowledgment is received from the other end.
    :param func: the function to decorate with. The name of the function is the event subscribers will subscribe to.
    :param strategy: The strategy of delivery. Can be 'RANDOM' or 'LEADER'. If 'RANDOM', then the event will be randomly
    passed to any one of the interested parties. If 'LEADER' then it is passed to the first instance alive
    which registered for that endpoint.
    """
    if func is None:
        return partial(xsubscribe, strategy=strategy)
    else:
        wrapper = _get_subscribe_decorator(func)
        wrapper.is_xsubscribe = True
        wrapper.strategy = strategy
        return wrapper


def _get_subscribe_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        coroutine_func = func
        if not iscoroutine(func):
            coroutine_func = coroutine(func)
        return (yield from coroutine_func(*args, **kwargs))

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


def api(func):  # incoming
    """
    provide a request/response api
    receives any requests here and return value is the response
    all functions must have the following signature
        - request_id
        - entity (partition/routing key)
        followed by kwargs
    """
    wrapper = _get_api_decorator(func)
    return wrapper


def apideprecated(func=None, replacement_api=None):
    if func is None:
        return partial(apideprecated, replacement_api=replacement_api)
    else:
        wrapper = _get_api_decorator(func=func, old_api=func.__name__, replacement_api=replacement_api)
        return wrapper


def _get_api_decorator(func=None, old_api=None, replacement_api=None):
    @coroutine
    @wraps(func)
    def wrapper(*args, **kwargs):
        _logger = logging.getLogger(__name__)
        start_time = int(time.time() * 1000)
        self = args[0]
        rid = kwargs.pop('request_id')
        entity = kwargs.pop('entity')
        from_id = kwargs.pop('from_id')
        wrapped_func = func
        result = None
        error = None
        failed = False

        status = 'succesful'
        success = True
        if not iscoroutine(func):
            wrapped_func = coroutine(func)

        Stats.tcp_stats['total_requests'] += 1

        try:
            result = yield from wait_for(wrapped_func(self, **kwargs), 120)

        except TimeoutError as e:
            Stats.tcp_stats['timedout'] += 1
            error = str(e)
            status = 'timeout'
            success = False
            failed = True

        except VykedServiceException as e:
            Stats.tcp_stats['total_responses'] += 1
            _logger.error(str(e))
            error = str(e)
            status = 'handled_error'

        except Exception as e:
            Stats.tcp_stats['total_errors'] += 1
            _logger.exception('api request exception')
            error = str(e)
            status = 'unhandled_error'
            success = False
            failed = True

        else:
            Stats.tcp_stats['total_responses'] += 1

        end_time = int(time.time() * 1000)

        hostname = socket.gethostname()
        service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])

        logd = {
            'endpoint': func.__name__,
            'time_taken': end_time - start_time,
            'hostname': hostname, 'service_name': service_name
        }
        logging.getLogger('stats').debug(logd)
        _logger.debug('Time taken for %s is %d milliseconds', func.__name__, end_time - start_time)

        # call to update aggregator, designed to replace the stats module.
        Aggregator.update_stats(endpoint=func.__name__, status=status, success=success,
                                server_type='tcp', time_taken=end_time - start_time)

        if not old_api:
            return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result,
                                              error=error, failed=failed)
        else:
            return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result,
                                              error=error, failed=failed, old_api=old_api,
                                              replacement_api=replacement_api)

    wrapper.is_api = True
    return wrapper


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
            elif isinstance(self, HTTPService):
                Stats.http_stats['total_requests'] += 1
                if required_params is not None:
                    req = args[0]
                    query_params = req.GET
                    params = required_params
                    if not isinstance(required_params, list):
                        params = [required_params]
                    missing_params = list(filter(lambda x: x not in query_params, params))
                    if len(missing_params) > 0:
                        res_d = {'error': 'Required params {} not found'.format(','.join(missing_params))}
                        Stats.http_stats['total_responses'] += 1
                        Aggregator.update_stats(endpoint=func.__name__, status=400, success=False,
                                                server_type='http', time_taken=0)
                        return Response(status=400, content_type='application/json', body=json.dumps(res_d).encode())

                t1 = time.time()
                wrapped_func = func
                success = True
                _logger = logging.getLogger()

                if not iscoroutine(func):
                    wrapped_func = coroutine(func)
                try:
                    result = yield from wait_for(wrapped_func(self, *args, **kwargs), 120)

                except TimeoutError as e:
                    Stats.http_stats['timedout'] += 1
                    logging.error("HTTP request had a %s" % str(e))
                    status = 'timeout'
                    success = False

                except VykedServiceException as e:
                    Stats.http_stats['total_responses'] += 1
                    _logger.error(str(e))
                    status = 'handled_exception'
                    raise e

                except Exception as e:
                    Stats.http_stats['total_errors'] += 1
                    _logger.exception('api request exception')
                    status = 'unhandled_exception'
                    success = False
                    raise e

                else:
                    t2 = time.time()
                    hostname = socket.gethostname()
                    service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])
                    status = result.status

                    logd = {
                        'status': result.status,
                        'time_taken': int((t2 - t1) * 1000),
                        'type': 'http',
                        'hostname': hostname, 'service_name': service_name
                    }
                    logging.getLogger('stats').debug(logd)
                    Stats.http_stats['total_responses'] += 1
                    return result

                finally:
                    t2 = time.time()
                    Aggregator.update_stats(endpoint=func.__name__, status=status, success=success,
                                            server_type='http', time_taken=int((t2 - t1) * 1000))

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

    @property
    def socket_address(self):
        return self._ip, self._port

    @property
    def host(self):
        return self._ip

    @property
    def port(self):
        return self._port

    def initiate(self):
        self.tcp_bus.register()
        yield from self.pubsub_bus.create_pubsub_handler()
        async(self.pubsub_bus.register_for_subscription(self.host, self.port, self.node_id, self.clients))


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

    @get('/ping')
    def pong(self, _):
        return Response()

    @get('/_stats')
    def stats(self, _):
        res_d = Aggregator.dump_stats()
        return Response(status=200, content_type='application/json', body=json.dumps(res_d).encode())


class HTTPServiceClient(_Service):
    def __init__(self, service_name, service_version):
        super(HTTPServiceClient, self).__init__(service_name, service_version)

    def _send_http_request(self, app_name, method, entity, params):
        response = yield from self._http_bus.send_http_request(app_name, self.name, self.version, method, entity,
                                                               params)
        return response
