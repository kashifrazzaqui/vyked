from functools import wraps, partial
from again.utils import unique_hex
from ..utils.stats import Stats, Aggregator
from ..exceptions import VykedServiceException

import asyncio
import logging
import socket
import setproctitle
import time


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


def deprecated(func=None, replacement_api=None):
    if func is None:
        return partial(deprecated, replacement_api=replacement_api)
    else:
        wrapper = _get_api_decorator(func=func, old_api=func.__name__, replacement_api=replacement_api)
        return wrapper


def _get_api_decorator(func=None, old_api=None, replacement_api=None):
    @asyncio.coroutine
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
        if not asyncio.iscoroutine(func):
            wrapped_func = asyncio.coroutine(func)

        Stats.tcp_stats['total_requests'] += 1

        try:
            result = yield from asyncio.wait_for(wrapped_func(self, **kwargs), 120)

        except asyncio.TimeoutError as e:
            Stats.tcp_stats['timedout'] += 1
            error = str(e)
            _logger.exception('api request timeout')
            status = 'timeout'
            success = False
            failed = True

        except VykedServiceException as e:
            Stats.tcp_stats['total_responses'] += 1
            _logger.exception(str(e))
            error = str(e)
            status = 'handled_error'

        except Exception as e:
            Stats.tcp_stats['total_errors'] += 1
            _logger.exception('api request unhandled exception')
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
