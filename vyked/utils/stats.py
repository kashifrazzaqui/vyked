import logging
import asyncio
import setproctitle
import socket

_logger = logging.getLogger('stats')


class Stats:
    hostname = socket.gethostname()
    service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])
    # hostd = {'hostname': '', 'service_name': ''}
    http_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0,
                  'type': 'httpservice', }
    tcp_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0,
                 'type': 'tcpservice', }

    @classmethod
    def periodic_stats_logger(cls):
        stats = dict(cls.http_stats)
        stats['hostname'] = cls.hostname
        stats['service_name'] = cls.service_name
        _logger.info(stats)

        stats = dict(cls.tcp_stats)
        stats['hostname'] = cls.hostname
        stats['service_name'] = cls.service_name
        _logger.info(stats)
        
        asyncio.get_event_loop().call_later(120, cls.periodic_stats_logger)
