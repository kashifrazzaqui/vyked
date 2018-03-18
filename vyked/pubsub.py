import logging
import asyncio
import json

import asyncio_redis as redis


class PubSub:

    """
    Pub sub handler which uses redis.
    Can be used to publish an event or subscribe to a list of endpoints
    """

    def __init__(self, redis_host, redis_port):
        """
        Create in instance of Pub Sub handler
        :param str redis_host: Redis Host address
        :param redis_port: Redis port number
        """
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._conn = None
        self._logger = logging.getLogger(__name__)
        self._is_blacklisted = False

    @asyncio.coroutine
    def connect(self):
        """
        Connect to the redis server and return the connection
        :return:
        """
        self._conn = yield from self._get_conn()
        return self._conn

    @asyncio.coroutine
    def publish(self, endpoint: str, payload: str):
        """
        Publish to an endpoint.
        :param str endpoint: Key by which the endpoint is recognised.
                         Subscribers will use this key to listen to events
        :param str payload: Payload to publish with the event
        :return: A boolean indicating if the publish was successful
        """
        if self._conn is not None:
            try:
                receiving_clients = yield from self._conn.publish(endpoint, payload)
                return receiving_clients
            except redis.Error as e:
                self._logger.error('Publish failed with error %s', repr(e))
        return 0

    @asyncio.coroutine
    def subscribe(self, endpoints: list, handler):
        """
        Subscribe to a list of endpoints
        :param endpoints: List of endpoints the subscribers is interested to subscribe to
        :type endpoints: list
        :param handler: The callback to call when a particular event is published.
                        Must take two arguments, a channel to which the event was published
                        and the payload.
        :return:
        """
        connection = yield from self._get_conn()
        subscriber = yield from connection.start_subscribe()
        yield from subscriber.subscribe(endpoints)
        while not self._is_blacklisted:
            payload = yield from subscriber.next_published()
            payload_value = json.loads(payload.value)
            payload_value.pop('_blocking', None)
            handler(payload.channel, json.dumps(payload_value))
        return False

    def _get_conn(self):
        return (yield from redis.Connection.create(self._redis_host, self._redis_port, auto_reconnect=True))

    @asyncio.coroutine
    def task_getter(self, endpoints, handler, blocking=False):
        connection = yield from self._get_conn()
        while not self._is_blacklisted:
            response = yield from connection.brpop(endpoints)
            queue_name, payload = response.list_name, response.value
            payload = json.loads(payload)
            blocking_sub = payload.pop('_blocking', False)
            payload = json.dumps(payload)
            if blocking or blocking_sub:
                try:
                    yield from handler(queue_name, payload, blocking)
                except:
                    pass
            else:
                try:
                    asyncio.async(handler(queue_name, payload))
                except:
                    pass

    @asyncio.coroutine
    def add_to_queue(self, endpoint: str, payload: str):
        if self._conn is not None:
            try:
                yield from self._conn.lpush(endpoint, [payload])
            except redis.Error as e:
                self._logger.error('Add to queue failed with error %s', repr(e))
        return 0
