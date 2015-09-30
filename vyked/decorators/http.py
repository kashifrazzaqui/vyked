from asyncio import iscoroutine, coroutine, wait_for, TimeoutError
from functools import wraps
from vyked import HTTPServiceClient, HTTPService
from ..exceptions import VykedServiceException
from aiohttp.web import Response
from ..utils.stats import Stats, Aggregator
import logging
import setproctitle
import socket
import json
import time


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
                    logging.exception("HTTP request had a %s" % str(e))
                    status = 'timeout'
                    success = False

                except VykedServiceException as e:
                    Stats.http_stats['total_responses'] += 1
                    _logger.exception(str(e))
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
