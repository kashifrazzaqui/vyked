from logging import Handler
from logging.handlers import RotatingFileHandler
from queue import Queue
import sys
import os
from threading import Thread
import logging
import asyncio
import datetime
from functools import partial, wraps

import configparser

FILE_SIZE = 5 * 1024 * 1024

LOGS_DIR = './logs'
LOG_FILE_NAME = 'vyked-{}.log'
LOG_LEVEL = logging.WARNING
FILE_PING_LOGS_ENABLED = False

RED = '\033[91m'
BLUE = '\033[94m'
BOLD = '\033[1m'
END = '\033[0m'

stream_handler = logging.StreamHandler()

pings_logs_enabled = True

STREAM_PING_LOGS_ENABLED = False
STREAM_LOG_LEVEL = logging.INFO

SOCKET_LOGS_ENABLED = False
SOCKET_LOG_HOST = 'localhost'
SOCKET_LOG_PORT = 9020
SOCKET_LOG_LEVEL = logging.DEBUG
SOCKET_PING_LOGS_ENABLED = True
SOCKET_LOG_BUFFER_SIZE = 10240
SOCKET_LOG_FLUSH_LEVEL = logging.ERROR


# Sample log.conf file:

# [ROTATING_FILE_HANDLER]
# LOGS_DIR = ./mylogdir
# LOG_FILE_NAME = mylogfor{}.log
# LOG_LEVEL = DEBUG
# #LOG_LEVEL can be {DEBUG, INFO, WARNING, ERROR, CRITICAL}
# FILE_SIZE = 5242880
# #5 MegaBytes
# PING_LOGS_ENABLED = True
# [STREAM_HANDLER]
# PING_LOGS_ENABLED = False
# LOG_LEVEL = CRITICAL
# #Pings will show only if log_level is appropriate
# [SOCKET_HANDLER]
# LOG_HOST = localhost
# LOG_PORT = 9020
# LOG_LEVEL = DEBUG
# PING_LOGS_ENABLED = False
# BUFFER_SIZE = 7
# #Flush after every 7 logs
# FLUSH_LEVEL = ERROR
# #Flush if ERROR log received

#Read Config File
LOG_CONF_FILE = ''

#Find log.conf file
for root, dirs, files in os.walk("."):
    for file in files:
        if file == 'log.conf':
             LOG_CONF_FILE = (os.path.join(root, file))
             break

#If file found update configurable parameters
if LOG_CONF_FILE!="":
    conf = configparser.ConfigParser()
    conf.read(LOG_CONF_FILE)
    #TODO: Add loop to update all logger handlers + datastructure to hold logger handler parameters
    #TODO: Possibly use default configuration file format
    if conf.has_section('ROTATING_FILE_HANDLER'):
        fileconfig = conf['ROTATING_FILE_HANDLER']
        LOGS_DIR = fileconfig.get('LOGS_DIR', LOGS_DIR)
        LOG_FILE_NAME = fileconfig.get('LOG_FILE_NAME', LOG_FILE_NAME)
        LOG_LEVEL = getattr(logging, fileconfig.get('LOG_LEVEL', 'INFO'))
        FILE_PING_LOGS_ENABLED = fileconfig.getboolean('PING_LOGS_ENABLED', FILE_PING_LOGS_ENABLED)
        FILE_SIZE = int(fileconfig.get('FILE_SIZE', '5242880'))

    if conf.has_section('ROTATING_FILE_HANDLER'):
        streamconfig = conf['STREAM_HANDLER']
        STREAM_PING_LOGS_ENABLED = streamconfig.getboolean('PING_LOGS_ENABLED', STREAM_PING_LOGS_ENABLED)
        STREAM_LOG_LEVEL = getattr(logging, streamconfig.get('LOG_LEVEL', 'INFO'))

    if conf.has_section('SOCKET_HANDLER'):
        socketconfig = conf['SOCKET_HANDLER']
        SOCKET_LOGS_ENABLED = True
        SOCKET_LOG_HOST = socketconfig.get('LOG_HOST','localhost')
        SOCKET_LOG_PORT = int(socketconfig.get('LOG_PORT','9020'))
        SOCKET_LOG_LEVEL = getattr(logging, socketconfig.get('LOG_LEVEL', 'DEBUG'))
        SOCKET_PING_LOGS_ENABLED = socketconfig.getboolean('PING_LOGS_ENABLED', SOCKET_PING_LOGS_ENABLED)
        SOCKET_LOG_BUFFER_SIZE = int(socketconfig.get('BUFFER_SIZE', '10240'))
        SOCKET_LOG_FLUSH_LEVEL = getattr(logging, socketconfig.get('FLUSH_LEVEL', 'DEBUG'))


#Helper function to filter ping/pong logs
def ping_filter(record):
    if '"type": "pong"' in record.getMessage() or '"type": "ping"' in record.getMessage():
        return 0
    return 1


class CustomTimeLoggingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        if datefmt:
            s = datetime.datetime.now().strftime(datefmt)
        else:
            t = datetime.datetime.now().strftime(self.default_time_format)
            s = self.default_msec_format % (t, record.msecs)
        return s


def is_ping_logging_enabled():
    return pings_logs_enabled


def config_logs(enable_ping_logs=False, log_level=logging.INFO):
    global pings_logs_enabled
    pings_logs_enabled = enable_ping_logs
    stream_handler.setLevel(log_level)


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
        formatter = CustomTimeLoggingFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                               '%Y-%m-%d %H:%M:%S,%f')
        async_handler.setFormatter(formatter)
        base_add_handler(async_handler)

    return async_add_handler


def create_logging_directory():
    log_dir = os.path.join(os.getcwd(), LOGS_DIR)
    if not os.path.exists(log_dir):
        os.mkdir(LOGS_DIR)


def setup_logging(identifier):
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    create_logging_directory()
    logger = logging.getLogger()
    logger.handlers = []
    logger.addHandler = patch_add_handler(logger)

    stream_handler.setLevel(STREAM_LOG_LEVEL)
    if not STREAM_PING_LOGS_ENABLED:
        stream_handler.addFilter(ping_filter)
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(os.path.join(LOGS_DIR, LOG_FILE_NAME.format(identifier)), maxBytes=FILE_SIZE,
                            backupCount=10)
    file_handler.setLevel(LOG_LEVEL)
    if not FILE_PING_LOGS_ENABLED:
        file_handler.addFilter(ping_filter)
    logger.addHandler(file_handler)

    if SOCKET_LOGS_ENABLED:
        web_handler = logging.handlers.SocketHandler(SOCKET_LOG_HOST, SOCKET_LOG_PORT)
        web_handler.setLevel(SOCKET_LOG_LEVEL)
        mem_handler = logging.handlers.MemoryHandler(SOCKET_LOG_BUFFER_SIZE, SOCKET_LOG_FLUSH_LEVEL, web_handler)
        mem_handler.setLevel(SOCKET_LOG_LEVEL)
        if not SOCKET_PING_LOGS_ENABLED:
            mem_handler.addFilter(ping_filter)
        mem_handler.setTarget(web_handler)
        logger.addHandler(mem_handler)


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
