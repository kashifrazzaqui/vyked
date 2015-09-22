import logging
import asyncio
import setproctitle
from collections import defaultdict
import socket

_logger = logging.getLogger('stats')


class Stats:
    hostname = socket.gethostname()
    service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])
    # hostd = {'hostname': '', 'service_name': ''}
    http_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0, 'total_errors': 0}
    tcp_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0, 'total_errors': 0}

    @classmethod
    def periodic_stats_logger(cls):
        logd = defaultdict(lambda: 0)
        logd['hostname'] = cls.hostname
        logd['service_name'] = cls.service_name

        for key, value in cls.http_stats.items():
            logd[key] += value
            logd['http_' + key] = value

        for key, value in cls.tcp_stats.items():
            logd[key] += value
            logd['tcp_' + key] = value

        _logger.info(dict(logd))

        asyncio.get_event_loop().call_later(120, cls.periodic_stats_logger)
