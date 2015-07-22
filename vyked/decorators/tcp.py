from asyncio import iscoroutinefunction, coroutine
from functools import wraps
from again.utils import unique_hex


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
            result = yield from wrapped_func(self, **kwargs)
        except BaseException as e:
            error = str(e)

        return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result, error=error)

    wrapper.is_api = True
    return wrapper
