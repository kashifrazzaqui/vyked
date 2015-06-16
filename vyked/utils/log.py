from logging import Handler
from queue import Queue
import sys
from threading import Thread
import logging
import asyncio
from functools import partial, wraps

RED = '\033[91m'
BLUE = '\033[94m'
BOLD = '\033[1m'
END = '\033[0m'


def patch_async_emit(handler: Handler):
    base_emit = handler.emit
    queue = Queue()

    def loop():
        while True:
            record = queue.get()
            try:
                base_emit(record)
            except:
                print(sys.exc_info())

    def async_emit(record):
        queue.put(record)

    thread = Thread(target=loop)
    thread.daemon = True
    thread.start()
    handler.emit = async_emit
    return handler


def patch_add_handler(logger):
    base_add_handler = logger.addHandler

    def async_add_handler(handler):
        async_handler = patch_async_emit(handler)
        base_add_handler(async_handler)

    return async_add_handler


def setup():
    logger = logging.getLogger()
    logger.handlers = []
    logger.addHandler = patch_add_handler(logger)
    handler = patch_async_emit(logging.StreamHandler())
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def log(fn=None, logger=logging.getLogger(), debug_level=logging.DEBUG):
    """
    logs parameters and result - takes no arguments
    """
    if fn is None:
        return partial(log, logger=logger, debug_level=debug_level)

    @wraps(fn)
    def func(*args, **kwargs):
        arg_string = ""
        for i in range(0, len(args)):
            var_name = fn.__code__.co_varnames[i]
            if var_name not in ['self', 'cls']:
                arg_string += var_name + ":" + str(args[i]) + ","
        arg_string = arg_string[0:len(arg_string) - 1]
        string = (RED + BOLD + '>> ' + END + 'Calling {0}({1})'.format(fn.__name__, arg_string))
        if len(kwargs):
            string = (
                RED + BOLD + '>> ' + END + 'Calling {0} with args {1} and kwargs {2}'.format(fn.__name__, arg_string,
                                                                                             kwargs))
        logger.log(debug_level, string)
        wrapped_fn = fn
        if not asyncio.iscoroutinefunction(fn):
            wrapped_fn = asyncio.coroutine(fn)
        try:
            result = yield from wrapped_fn(*args, **kwargs)
            string = BLUE + BOLD + '<< ' + END + 'Return {0} with result :{1}'.format(fn.__name__, result)
            logger.log(debug_level, string)
            return result
        except Exception as e:
            string = (RED + BOLD + '>> ' + END + '{0} raised exception :{1}'.format(fn.__name__, str(e)))
            logger.log(debug_level, string)
            raise e

    return func


def logx(supress_args=[], supress_all_args=False, supress_result=False, logger=logging.getLogger(),
         debug_level=logging.DEBUG):
    """
    logs parameters and result
    takes arguments
        supress_args - list of parameter names to supress
        supress_all_args - boolean to supress all arguments
        supress_result - boolean to supress result
        receiver - custom logging function which takes a string as input; defaults to logging on stdout
    """

    def decorator(fn):
        def func(*args, **kwargs):
            if not supress_all_args:
                arg_string = ""
                for i in range(0, len(args)):
                    var_name = fn.__code__.co_varnames[i]
                    if var_name != "self" and var_name not in supress_args:
                        arg_string += var_name + ":" + str(args[i]) + ","
                arg_string = arg_string[0:len(arg_string) - 1]
                string = (RED + BOLD + '>> ' + END + 'Calling {0}({1})'.format(fn.__name__, arg_string))
                if len(kwargs):
                    string = (
                        RED + BOLD + '>> ' + END + 'Calling {0} with args {1} and kwargs {2}'.format(
                            fn.__name__,
                            arg_string, kwargs))
                logger.log(debug_level, string)

            wrapped_fn = fn
            if not asyncio.iscoroutinefunction(fn):
                wrapped_fn = asyncio.coroutine(fn)
            result = yield from wrapped_fn(*args, **kwargs)

            if not supress_result:
                string = BLUE + BOLD + '<< ' + END + 'Return {0} with result : {1}'.format(fn.__name__, result)
                logger.log(debug_level, string)
            return result

        return func

    return decorator


