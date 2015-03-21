from functools import wraps

try:
    import cPickle as pickle
except ImportError:
    import pickle

from redis import Redis


def key_for_name(name):
    return 'publishqueue:{}'.format(name)


class PublishQueue(object):
    def __init__(self, name, serializer=pickle, **kwargs):
        self.name = name
        self.serializer = serializer
        self.__redis = Redis(**kwargs)

    def __len__(self):
        return self.__redis.llen(self.key)

    @property
    def key(self):
        return key_for_name(self.name)

    def clear(self):
        self.__redis.delete(self.key)

    def consume(self, **kwargs):
        kwargs.setdefault('block', True)
        try:
            while True:
                msg = self.get(**kwargs)
                if msg is None:
                    break
                yield msg
        except KeyboardInterrupt:
            return

    def get(self, block=False, timeout=None):
        if block:
            if timeout is None:
                timeout = 0
            msg = self.__redis.blpop(self.key, timeout=timeout)
            if msg is not None:
                msg = msg[1]
        else:
            msg = self.__redis.lpop(self.key)
        if msg is not None and self.serializer is not None:
            msg = self.serializer.loads(msg)
        return msg

    def put(self, *msgs):
        if self.serializer is not None:
            msgs = map(self.serializer.dumps, msgs)
        self.__redis.rpush(self.key, *msgs)

    def worker(self, *args, **kwargs):
        def decorator(worker):
            @wraps(worker)
            def wrapper(*args):
                for msg in self.consume(**kwargs):
                    worker(*args + (msg,))

            return wrapper

        if args:
            return decorator(*args)
        return decorator
