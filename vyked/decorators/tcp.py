from functools import wraps, partial
from again.utils import unique_hex
from ..utils.stats import Stats, Aggregator
from ..exceptions import VykedServiceException, RequestException
from ..utils.common_utils import json_file_to_dict, valid_timeout, tcp_to_http_path_for_function, object_to_dict
from ..utils.jsonencoder import VykedEncoder
from ..config import CONFIG
from .http import get_decorated_fun_for_tcp_to_http
from ..wrappers import Request, Response
from ..host import Host
import asyncio
import logging
import socket
import setproctitle
import time
import traceback
import json
import inspect

config = json_file_to_dict('config.json')
_tcp_timeout = 60

if isinstance(config, dict) and 'TCP_TIMEOUT' in config and valid_timeout(config['TCP_TIMEOUT']):
    _tcp_timeout = config['TCP_TIMEOUT']


def publish(func=None, blocking=False):
    """
    publish the return value of this function as a message from this endpoint
    """
    if func is None:
        return partial(publish, blocking=blocking)
    @wraps(func)
    def wrapper(self, *args, **kwargs):  # outgoing
        payload = func(self, *args, **kwargs)
        payload.pop('self', None)
        self._publish(func.__name__, payload, blocking=blocking)
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


def xsubscribe(func=None, strategy='DESIGNATION', blocking=False):
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
        return partial(xsubscribe, strategy=strategy, blocking=blocking)
    else:
        wrapper = _get_subscribe_decorator(func)
        wrapper.is_xsubscribe = True
        wrapper.strategy = strategy
        wrapper.blocking = blocking
        return wrapper


def _get_subscribe_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        coroutine_func = func
        if not asyncio.iscoroutine(func):
            coroutine_func = asyncio.coroutine(func)
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
        if CONFIG.Convert_Tcp_To_Http:
            post_params = {'data':params, 'path': tcp_to_http_path_for_function(func) }
            response =  self._send_http_request(app_name, method='post', entity=entity, params=post_params)
            return response
        else:
            params['request_id'] = request_id
            future = self._send_request(app_name, endpoint=func.__name__, entity=entity, params=params)
            return future
    wrapper.is_request = True
    return wrapper

def tcp_to_http_handler(func, obj):
    def handler(obj, request: Request) -> Response:
        request_params = yield from request.json()
        payload = request_params.pop('payload', {})
        logging.debug("recieved tcp_to_http request {}".format(request_params))
        required_params = inspect.getfullargspec(func).args[1:]
        missing_params = list(filter(lambda x: x not in payload, required_params))
        if missing_params:
            res_d = {'error': 'Required params {} not found'.format(','.join(missing_params))}
            raise RequestException(res_d['error'])
        result = yield from func(Host._tcp_service, **payload)
        return result
    return handler


def api(func=None, timeout=None):  # incoming
    """
    provide a request/response api
    receives any requests here and return value is the response
    all functions must have the following signature
        - request_id
        - entity (partition/routing key)
        followed by kwargs
    """
    #logging.info("convert tcp to http {}".format(CONFIG.Convert_Tcp_To_Http))
    if CONFIG.Convert_Tcp_To_Http:
        transform_func = tcp_to_http_handler(func, Host._http_service)
        return get_decorated_fun_for_tcp_to_http(transform_func,'post', tcp_to_http_path_for_function(func), None, timeout)
    if func is None:
        return partial(api, timeout=timeout)
    else:
        wrapper = _get_api_decorator(func=func, timeout=timeout)
        return wrapper


def deprecated(func=None, replacement_api=None):
    if func is None:
        return partial(deprecated, replacement_api=replacement_api)
    else:
        wrapper = _get_api_decorator(func=func, old_api=func.__name__, replacement_api=replacement_api)
        return wrapper


def _get_api_decorator(func=None, old_api=None, replacement_api=None, timeout=None):
    @asyncio.coroutine
    @wraps(func)
    def wrapper(*args, **kwargs):
        _logger = logging.getLogger(__name__)
        start_time = int(time.time() * 1000)
        start_process_time = int(time.process_time() * 1000)
        self = args[0]
        rid = kwargs.pop('request_id')
        entity = kwargs.pop('entity')
        from_id = kwargs.pop('from_id')
        wrapped_func = func
        result = None
        error = None
        failed = False
        api_timeout = _tcp_timeout

        status = 'succesful'
        success = True
        if not asyncio.iscoroutine(func):
            wrapped_func = asyncio.coroutine(func)

        if valid_timeout(timeout):
            api_timeout = timeout

        Stats.tcp_stats['total_requests'] += 1

        try:
            result = yield from asyncio.wait_for(asyncio.shield(wrapped_func(self, **kwargs)), api_timeout)

        except asyncio.TimeoutError as e:
            Stats.tcp_stats['timedout'] += 1
            error = str(e)
            status = 'timeout'
            success = False
            failed = True
            logging.exception("TCP request had a timeout for method %s", func.__name__)

        except VykedServiceException as e:
            Stats.tcp_stats['total_responses'] += 1
            error = str(e)
            status = 'handled_error'
            _logger.info('Handled exception %s for method %s ', e.__class__.__name__, func.__name__)

        except Exception as e:
            Stats.tcp_stats['total_errors'] += 1
            error = str(e)
            status = 'unhandled_error'
            success = False
            failed = True
            _logger.exception('Unhandled exception %s for method %s ', e.__class__.__name__, func.__name__)
            _stats_logger = logging.getLogger('stats')
            _method_param = json.dumps(kwargs)
            d = {"exception_type": e.__class__.__name__, "method_name": func.__name__, "message": str(e),
                 "method_param": _method_param, "service_name": self._service_name,
                 "hostname": socket.gethostbyname(socket.gethostname())}
            _stats_logger.info(dict(d))
            _exception_logger = logging.getLogger('exceptions')
            d["message"] = traceback.format_exc()
            _exception_logger.info(dict(d))

        else:
            Stats.tcp_stats['total_responses'] += 1

        end_time = int(time.time() * 1000)
        end_process_time = int(time.process_time() * 1000)

        hostname = socket.gethostname()
        service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])

        logd = {
            'endpoint': func.__name__,
            'time_taken': end_time - start_time,
            'hostname': hostname, 'service_name': service_name
        }
        logging.getLogger('stats').debug(logd)
        _logger.debug('Time taken for %s is %d milliseconds', func.__name__, end_time - start_time)
        _logger.debug('Timeout for %s is %s seconds', func.__name__, api_timeout)

        # call to update aggregator, designed to replace the stats module.
        Aggregator.update_stats(endpoint=func.__name__, status=status, success=success,
                                server_type='tcp', time_taken=end_time - start_time,
                                process_time_taken=end_process_time - start_process_time)

        if not old_api:
            return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result,
                                              error=error, failed=failed, method=func.__name__,
                                              service_name=self.name)
        else:
            return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result,
                                              error=error, failed=failed, old_api=old_api,
                                              replacement_api=replacement_api, method=func.__name__,
                                              service_name=self.name)

    wrapper.is_api = True
    return wrapper


def task_queue(func=None, queue_name=None):
    if func is None:
        return partial(task_queue, queue_name=queue_name)
    @wraps(func)
    def wrapper(*args, **kwargs):
        coroutine_func = func
        if not asyncio.iscoroutine(func):
            coroutine_func = asyncio.coroutine(func)
        return (yield from coroutine_func(*args, **kwargs))
    wrapper.queue_name = queue_name
    wrapper.is_task_queue = True
    return wrapper


def enqueue(func=None, queue_name=None):
    if func is None:
        return partial(enqueue, queue_name=queue_name)
    @wraps(func)
    def wrapper(self, *args, **kwargs):  # outgoing
        payload = func(self, *args, **kwargs)
        payload.pop('self', None)
        self._enqueue(queue_name, payload)
        return None
    return wrapper
















