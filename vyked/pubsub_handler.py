import json
import logging
import asyncio

import asyncio_redis as redis
from vyked.utils.log import log

class PubSubHandler:
    _logger = logging.getLogger(__name__)

    def __init__(self, redis_host, redis_port):
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._conn = None

    @asyncio.coroutine
    def connect(self):
        self._conn = yield from self._get_conn()
        return self._conn

    @asyncio.coroutine
    @log(logger=_logger)
    def publish(self, service, version, endpoint, payload):
        if self._conn is not None:
            try:
                yield from self._conn.publish(self._get_key(service, version, endpoint), json.dumps(payload))
                return True
            except redis.Error as e:
                self._logger.error('Publish failed with error %s', repr(e))
        return False

    @asyncio.coroutine
    def subscribe(self, endpoints, handler):
        connection = yield from self._get_conn()
        subscriber = yield from connection.start_subscribe()
        channels = [self._get_key(*endpoint) for endpoint in endpoints]
        yield from subscriber.subscribe(channels)
        while True:
            payload = yield from subscriber.next_published()
            service, version, endpoint = payload.channel.split('.')
            handler(service, int(version), endpoint, payload.value)
        return False

    @staticmethod
    def _get_key(service, version, endpoint):
        return '{}.{}.{}'.format(service, version, endpoint)

    def _get_conn(self):
        return (yield from redis.Connection.create(self._redis_host, self._redis_port, auto_reconnect=True))






