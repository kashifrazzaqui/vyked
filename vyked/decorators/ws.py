from asyncio import iscoroutine, coroutine
from functools import wraps
from vyked import WSService


def get_decorated_fun(method, path):
    def decorator(func):
        @wraps(func)
        def f(self, *args, **kwargs):
            if isinstance(self, WSService):
                wrapped_func = func
                if not iscoroutine(func):
                    wrapped_func = coroutine(func)
                return (yield from wrapped_func(self, *args, **kwargs))
        f.is_ws_method = True
        f.method = method
        f.paths = path
        if not isinstance(path, list):
            f.paths = [path]
        return f

    return decorator

def ws(path=None):
    return get_decorated_fun('get', path)