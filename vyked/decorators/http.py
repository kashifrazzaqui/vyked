from asyncio import iscoroutine, coroutine, wait_for, TimeoutError, shield
from functools import wraps
from vyked import HTTPServiceClient, HTTPService, TCPService
from ..exceptions import VykedServiceException
from aiohttp.web import Response
from ..utils.stats import Stats, Aggregator
from ..utils.jsonencoder import VykedEncoder
from ..utils.common_utils import json_file_to_dict, valid_timeout
import logging
import setproctitle
import socket
import json
import time
import traceback

config = json_file_to_dict('config.json')
_http_timeout = 60

if isinstance(config, dict) and 'HTTP_TIMEOUT' in config and valid_timeout(config['HTTP_TIMEOUT']):
    _http_timeout = config['HTTP_TIMEOUT']

def make_request(func, self, args, kwargs, method):
    params = func(self, *args, **kwargs)
    entity = params.pop('entity', None)
    app_name = params.pop('app_name', None)
    self = params.pop('self')
    response = yield from self._send_http_request(app_name, method, entity, params)
    return response


def get_decorated_fun(method, path, required_params, timeout):
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
                                                server_type='http', time_taken=0, process_time_taken=0)
                        return Response(status=400, content_type='application/json', body=json.dumps(res_d).encode())

                # Support for multi request body encodings
                req = args[0]
                try:
                    yield from req.json()
                except:
                    pass
                else:
                    req.post = req.json
                t1 = time.time()
                tp1 = time.process_time()
                wrapped_func = func
                success = True
                _logger = logging.getLogger()
                api_timeout = _http_timeout

                if valid_timeout(timeout):
                    api_timeout = timeout

                if not iscoroutine(func):
                    wrapped_func = coroutine(func)

                try:
                    result = yield from wait_for(shield(wrapped_func(self, *args, **kwargs)), api_timeout)

                except TimeoutError as e:
                    Stats.http_stats['timedout'] += 1
                    status = 'timeout'
                    success = False
                    _logger.exception("HTTP request had a timeout for method %s", func.__name__)
                    raise e

                except VykedServiceException as e:
                    Stats.http_stats['total_responses'] += 1
                    status = 'handled_exception'
                    _logger.info('Handled exception %s for method %s ', e.__class__.__name__, func.__name__)
                    raise e

                except Exception as e:
                    Stats.http_stats['total_errors'] += 1
                    status = 'unhandled_exception'
                    success = False
                    _logger.exception('Unhandled exception %s for method %s ', e.__class__.__name__, func.__name__)
                    _stats_logger = logging.getLogger('stats')
                    d = {"exception_type": e.__class__.__name__, "method_name": func.__name__, "message": str(e),
                         "service_name": self._service_name, "hostname": socket.gethostbyname(socket.gethostname())}
                    _stats_logger.info(dict(d))
                    _exception_logger = logging.getLogger('exceptions')
                    d["message"] = traceback.format_exc()
                    _exception_logger.info(dict(d))
                    raise e
                    
                else:
                    t2 = time.time()
                    tp2 = time.process_time()
                    hostname = socket.gethostname()
                    service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])
                    status = result.status

                    logd = {
                        'status': result.status,
                        'time_taken': int((t2 - t1) * 1000),
                        'process_time_taken': int((tp2-tp1) * 1000),
                        'type': 'http',
                        'hostname': hostname, 'service_name': service_name
                    }
                    logging.getLogger('stats').debug(logd)
                    _logger.debug('Timeout for %s is %s seconds', func.__name__, api_timeout)
                    Stats.http_stats['total_responses'] += 1
                    return result

                finally:
                    t2 = time.time()
                    tp2 = time.process_time()
                    Aggregator.update_stats(endpoint=func.__name__, status=status, success=success,
                                            server_type='http', time_taken=int((t2 - t1) * 1000),
                                            process_time_taken=int((tp2 - tp1) * 1000))

        f.is_http_method = True
        f.method = method
        f.paths = path
        if not isinstance(path, list):
            f.paths = [path]
        return f
    return decorator


def get_decorated_fun_for_tcp_to_http(func, method, path, required_params, timeout):
        @wraps(func)
        def f(self, *args, **kwargs):
            if isinstance(self, HTTPServiceClient):
                return (yield from make_request(func, self, args, kwargs, method))
            elif isinstance(self, HTTPService) or isinstance(self, TCPService):
                Stats.http_stats['total_requests'] += 1
                req = args[0]
                request_id = None
                entity = None
                try:
                    req_b = yield from req.json()
                    request_id = req_b.get('pid', None)
                    entity = req_b.get('entity', 'packet')
                except :
                    pass
                else:
                    req.post = req.json
                if required_params is not None:
                    query_params = req.GET
                    params = required_params
                    if not isinstance(required_params, list):
                        params = [required_params]
                    missing_params = list(filter(lambda x: x not in query_params, params))
                    if len(missing_params) > 0:
                        res_d = {'error': 'Required params {} not found'.format(','.join(missing_params))}
                        Stats.http_stats['total_responses'] += 1
                        Aggregator.update_stats(endpoint=func.__name__, status=400, success=False,
                                                server_type='http', time_taken=0, process_time_taken=0)
                        resp_packet =  self._make_response_packet(request_id=request_id, from_id=self.node_id, entity=entity, result=None,
                                                                  error=res_d['error'], failed=True, method=func.__name__,
                                                                  service_name=self.name )
                        return Response(status=400, content_type='application/json', body=json.dumps(resp_packet, cls=VykedEncoder).encode())

                # Support for multi request body encodings


                t1 = time.time()
                tp1 = time.process_time()
                wrapped_func = func
                success = True
                _logger = logging.getLogger()
                api_timeout = _http_timeout

                if valid_timeout(timeout):
                    api_timeout = timeout

                if not iscoroutine(func):
                    wrapped_func = coroutine(func)

                try:
                    result = yield from wait_for(shield(wrapped_func(self, *args, **kwargs)), api_timeout)

                except TimeoutError as e:
                    Stats.http_stats['timedout'] += 1
                    status = 'timeout'
                    success = False
                    _logger.exception("HTTP request had a timeout for method %s", func.__name__)
                    resp_packet = self._make_response_packet(request_id=request_id, from_id=self.node_id, entity=entity,
                                                             result=None,
                                                             error=str(e), failed=True, method=func.__name__,
                                                             service_name=self.name)
                    return Response(status=408, content_type='application/json',
                                    body=json.dumps(resp_packet, cls=VykedEncoder).encode())

                except VykedServiceException as e:
                    Stats.http_stats['total_responses'] += 1
                    status = 'handled_exception'
                    _logger.info('Handled exception %s for method %s ', e.__class__.__name__, func.__name__)
                    resp_packet = self._make_response_packet(request_id=request_id, from_id=self.node_id, entity=entity,
                                                             result=None,
                                                             error=str(e), failed=True, method=func.__name__,
                                                             service_name=self.name)
                    return Response(status=400, content_type='application/json',
                                    body=json.dumps(resp_packet, cls=VykedEncoder).encode())

                except Exception as e:
                    Stats.http_stats['total_errors'] += 1
                    status = 'unhandled_exception'
                    success = False
                    _logger.exception('Unhandled exception %s for method %s ', e.__class__.__name__, func.__name__)
                    _stats_logger = logging.getLogger('stats')
                    d = {"exception_type": e.__class__.__name__, "method_name": func.__name__, "message": str(e),
                         "service_name": self._service_name, "hostname": socket.gethostbyname(socket.gethostname())}
                    _stats_logger.info(dict(d))
                    _exception_logger = logging.getLogger('exceptions')
                    d["message"] = traceback.format_exc()
                    _exception_logger.info(dict(d))
                    resp_packet = self._make_response_packet(request_id=request_id, from_id=self.node_id, entity=entity,
                                                             result=None,
                                                             error=str(e), failed=True, method=func.__name__,
                                                             service_name=self.name)
                    return Response(status=400, content_type='application/json',
                                    body=json.dumps(resp_packet, cls=VykedEncoder).encode())

                else:
                    t2 = time.time()
                    tp2 = time.process_time()
                    hostname = socket.gethostname()
                    service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])
                    status = getattr(result,'status', 200)
                    logd = {
                        'status': status,
                        'time_taken': int((t2 - t1) * 1000),
                        'process_time_taken': int((tp2 - tp1) * 1000),
                        'type': 'http',
                        'hostname': hostname, 'service_name': service_name
                    }
                    logging.getLogger('stats').debug(logd)
                    _logger.debug('Timeout for %s is %s seconds', func.__name__, api_timeout)
                    Stats.http_stats['total_responses'] += 1
                    resp_packet = self._make_response_packet(request_id=request_id, from_id=self.node_id, entity=entity,
                                                             result=result,
                                                             error=None, failed=False, method=func.__name__,
                                                             service_name=self.name)
                    return Response(status=status, content_type='application/json', body=json.dumps(resp_packet, cls=VykedEncoder).encode())

                finally:
                    t2 = time.time()
                    tp2 = time.process_time()
                    Aggregator.update_stats(endpoint=func.__name__, status=status, success=success,
                                            server_type='http', time_taken=int((t2 - t1) * 1000),
                                            process_time_taken=int((tp2 - tp1) * 1000))

        f.is_http_method = True
        f.method = method
        f.paths = path
        if not isinstance(path, list):
            f.paths = [path]
        return f



def get(path=None, required_params=None, timeout=None):
    return get_decorated_fun('get', path, required_params, timeout)


def head(path=None, required_params=None, timeout=None):
    return get_decorated_fun('head', path, required_params, timeout)


def options(path=None, required_params=None, timeout=None):
    return get_decorated_fun('options', path, required_params, timeout)


def patch(path=None, required_params=None, timeout=None):
    return get_decorated_fun('patch', path, required_params, timeout)


def post(path=None, required_params=None, timeout=None):
    return get_decorated_fun('post', path, required_params, timeout)


def put(path=None, required_params=None, timeout=None):
    return get_decorated_fun('put', path, required_params, timeout)


def trace(path=None, required_params=None, timeout=None):
    return get_decorated_fun('put', path, required_params, timeout)


def delete(path=None, required_params=None, timeout=None):
    return get_decorated_fun('delete', path, required_params, timeout)
