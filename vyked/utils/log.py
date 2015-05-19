from logging import Handler
from queue import Queue
import sys
from threading import Thread
import logging


def patch_async_emit(handler : Handler):
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


def setup():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    handler = patch_async_emit(logging.StreamHandler())
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

