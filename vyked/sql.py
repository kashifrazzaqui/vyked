from asyncio import coroutine
from functools import wraps

from aiopg import create_pool, Pool, Cursor

class SQLStore:
    _pool = None
    _connection_params = {}

    @classmethod
    def connect(cls, database, user, password, host, port):
        cls._connection_params['database'] = database
        cls._connection_params['user'] = user
        cls._connection_params['password'] = password
        cls._connection_params['host'] = host
        cls._connection_params['port'] = port

    @classmethod
    def use_pool(cls, pool):
        cls._pool = pool

    @classmethod
    @coroutine
    def get_pool(cls) -> Pool:
        if len(cls._connection_params) < 5:
            raise ConnectionError('Please call SQLStore.connect before calling this method')
        if not cls._pool:
            cls._pool = yield from create_pool(**cls._connection_params)
        return cls._pool

    @classmethod
    @coroutine
    def get_cursor(cls) -> Cursor:
        pool = yield from cls.get_pool()
        cursor = yield from pool.cursor()
        return cursor


def cursor(func):
    """
    decorator that provides a cursor from a pool and executes the function within its context manager
    """

    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        with (yield from cls.get_cursor()) as c:
            yield from func(cls, c, *args, **kwargs)

    return wrapper
