import logging
import asyncio

_logger = logging.getLogger('stats')


class Stats:
    http_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0, 'type': 'httpservice'}
    tcp_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0, 'type': 'tcpservice'}

    @classmethod
    def periodic_stats_logger(cls):
        _logger.info(dict(cls.http_stats))
        _logger.info(dict(cls.tcp_stats))
        asyncio.get_event_loop().call_later(120, cls.periodic_stats_logger)
