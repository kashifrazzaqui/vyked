from asyncio import iscoroutine, coroutine
from functools import wraps
from vyked import HTTPServiceClient, HTTPService
from aiohttp.web import Response
import logging
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
                if required_params is not None:
                    req = args[0]
                    query_params = req.GET
                    params = required_params
                    if not isinstance(required_params, list):
                        params = [required_params]
                    missing_params = list(filter(lambda x: x not in query_params, params))
                    if len(missing_params) > 0:
                        return Response(status=400, content_type='application/json',
                                        body=json.dumps({'error': 'Required params {} not found'.format(
                                            ','.join(missing_params))}).encode())
                t1 = time.time()
                wrapped_func = func
                if not iscoroutine(func):
                    wrapped_func = coroutine(func)
                result = yield from wrapped_func(self, *args, **kwargs)
                t2 = time.time()
                logd = {
                    'status': result.status,
                    'time_taken': int((t2 - t1)*1000),
                    'type': 'http',
                }
                logging.getLogger('apilog').info(logd)

                return (result)

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
