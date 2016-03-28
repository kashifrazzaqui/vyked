from logging import Handler
from queue import Queue
from threading import Thread
import logging.config
import logging
import asyncio
import datetime
import yaml
import sys

from functools import partial, wraps
from pythonjsonlogger import jsonlogger


RED = '\033[91m'
BLUE = '\033[94m'
BOLD = '\033[1m'
END = '\033[0m'


class CustomTimeLoggingFormatter(logging.Formatter):

    def formatTime(self, record, datefmt=None):  # noqa
        """
        Overrides formatTime method to use datetime module instead of time module
        to display time in microseconds. Time module by default does not resolve
        time to microseconds.
        """
        if datefmt:
            s = datetime.datetime.now().strftime(datefmt)
        else:
            t = datetime.datetime.now().strftime(self.default_time_format)
            s = self.default_msec_format % (t, record.msecs)
        return s


class CustomJsonFormatter(jsonlogger.JsonFormatter):

    def __init__(self, *args, **kwargs):
        self.extrad = kwargs.pop('extrad', {})
        super().__init__(*args, **kwargs)

    def add_fields(self, log_record, record, message_dict):
        message_dict.update(self.extrad)
        super().add_fields(log_record, record, message_dict)


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


DEFAULT_CONFIG_YAML = """
    # logging config

    version: 1
    disable_existing_loggers: False
    handlers:
        stream:
            class: logging.StreamHandler
            level: INFO
            formatter: ctf
            stream: ext://sys.stdout

        stats:
            class: logging.FileHandler
            level: INFO
            formatter: cjf
            filename: logs/vyked_stats.log

        service:
            class: logging.FileHandler
            level: INFO
            formatter: ctf
            filename: logs/vyked_service.log

    formatters:
        ctf:
            (): vyked.utils.log.CustomTimeLoggingFormatter
            format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            datefmt: '%Y-%m-%d %H:%M:%S,%f'

        cjf:
            (): vyked.utils.log.CustomJsonFormatter
            format: '{ "timestamp":"%(asctime)s", "message":"%(message)s"}'
            datefmt: '%Y-%m-%d %H:%M:%S,%f'

    root:
        handlers: [stream, service]
        level: INFO

    loggers:
        registry:
            handlers: [service,]
            level: INFO

        stats:
            handlers: [stats]
            level: INFO

    """


def setup_logging(_):
    try:
        with open('config_log.json', 'r') as f:
            config_dict = yaml.load(f.read())
    except:
        config_dict = yaml.load(DEFAULT_CONFIG_YAML)

    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logger = logging.getLogger()
    logger.handlers = []
    logger.addHandler = patch_add_handler(logger)

    logging.config.dictConfig(config_dict)


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
        if not asyncio.iscoroutine(fn):
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
            if not asyncio.iscoroutine(fn):
                wrapped_fn = asyncio.coroutine(fn)
            result = yield from wrapped_fn(*args, **kwargs)

            if not supress_result:
                string = BLUE + BOLD + '<< ' + END + 'Return {0} with result : {1}'.format(fn.__name__, result)
                logger.log(debug_level, string)
            return result

        return func

    return decorator
