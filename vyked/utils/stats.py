import logging
import asyncio
import setproctitle
from collections import defaultdict
import socket


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

        _logger = logging.getLogger('stats')
        _logger.info(dict(logd))

        asyncio.get_event_loop().call_later(120, cls.periodic_stats_logger)


class StatUnit:
    MAXSIZE = 10

    def __init__(self, key=None):
        self.key = key
        self.average = 0
        self.values = list()
        self.count = 0
        self.success_count = 0
        self.sub = dict()

    def update(self, val, success):
        self.values.append(val)
        if len(self.values) > self.MAXSIZE:
            self.values.pop(0)

        self.average = sum(self.values) / len(self.values)
        self.count += 1
        if success:
            self.success_count += 1

    def to_dict(self):

        d = dict({'count': self.count, 'average': self.average, 'success_count': self.success_count, 'sub': dict()})
        for k, v in self.sub.items():
            d['sub'][k] = v.to_dict()
        return d

    def __str__(self):
        return "{} {} {} {}".format(self.key, self.sum, self.average, self.count)


class Aggregator:
    _stats = StatUnit(key='total')

    @classmethod
    def recursive_update(cls, d, new_val, keys, success):
        if len(keys) == 0:
            return

        try:
            key = keys.pop()
            value = d[key]

        except KeyError:
            value = StatUnit(key=key)
            d[key] = value

        finally:
            value.update(new_val, success)
            cls.recursive_update(value.sub, new_val, keys, success)

    @classmethod
    def update_stats(cls, endpoint, status, time_taken, server_type, success=True):

        cls._stats.update(val=time_taken, success=success)
        cls.recursive_update(cls._stats.sub, time_taken, keys=[status, endpoint, server_type], success=success)

    @classmethod
    def dump_stats(cls):
        return cls._stats.to_dict()

    @classmethod
    def periodic_aggregated_stats_logger(cls):
        hostname = socket.gethostname()
        service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])

        logd = cls._stats.to_dict()
        logs = []
        for server_type in ['http', 'tcp']:
            try:
                server_type_d = logd['sub'][server_type]['sub']
            except KeyError:
                continue
            for k, v in server_type_d.items():
                d = dict({
                    'method': k,
                    'server_type': server_type,
                    'hostname': hostname,
                    'service_name': service_name,
                    'average_response_time': v['average'],
                    'total_request_count': v['count'],
                    'success_count': v['success_count']
                })
                for k2, v2 in v['sub'].items():
                    d['CODE_{}'.format(k2)] = v2['count']
                logs.append(d)

        _logger = logging.getLogger('stats')
        for logd in logs:
            _logger.info(dict(logd))

        asyncio.get_event_loop().call_later(300, cls.periodic_aggregated_stats_logger)
