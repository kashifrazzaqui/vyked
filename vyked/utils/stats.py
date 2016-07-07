import logging
import asyncio
import setproctitle
from collections import defaultdict
import socket
import resource


class Stats:
    rusage_denom = 1024.

    hostname = socket.gethostbyname(socket.gethostname())
    service_name = '_'.join(setproctitle.getproctitle().split('_')[1:-1])
    # hostd = {'hostname': '', 'service_name': ''}
    http_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0, 'total_errors': 0}
    tcp_stats = {'total_requests': 0, 'total_responses': 0, 'timedout': 0, 'total_errors': 0}

    @classmethod
    def periodic_stats_logger(cls):
        logd = defaultdict(lambda: 0)
        logd['hostname'] = cls.hostname
        logd['service_name'] = cls.service_name
        logd['mem_usage'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / cls.rusage_denom

        for key, value in cls.http_stats.items():
            logd[key] += value
            logd['http_' + key] = value
            cls.http_stats[key] = 0

        for key, value in cls.tcp_stats.items():
            logd[key] += value
            logd['tcp_' + key] = value
            cls.tcp_stats[key] = 0

        _logger = logging.getLogger('stats')
        _logger.info(dict(logd))

        asyncio.get_event_loop().call_later(120, cls.periodic_stats_logger)


class StatUnit:
    MAXSIZE = 10

    def __init__(self, key=None):
        self.key = key
        self.average = 0
        self.process_time_average = 0
        self.values = list()
        self.count = 0
        self.success_count = 0
        self.sub = dict()

    def update(self, val, process_time_taken, success):
        self.values.append(val)
        if len(self.values) > self.MAXSIZE:
            self.values.pop(0)

        self.count += 1
        if success:
            self.average = (self.average * self.success_count + val)/(self.success_count+1)
            self.process_time_average = (self.process_time_average * self.success_count + process_time_taken)/(self.success_count+1)
            self.success_count += 1

    def to_dict(self):

        d = dict({'count': self.count, 'average': int(self.average), 'success_count': self.success_count, 'sub': dict(),
            'process_time_average': self.process_time_average})
        for k, v in self.sub.items():
            d['sub'][k] = v.to_dict()
        return d

    def __str__(self):
        return "{} {} {} {}".format(self.key, self.sum, self.average, self.count)


class Aggregator:
    _stats = StatUnit(key='total')

    @classmethod
    def recursive_update(cls, d, new_val, keys, success, process_time_taken=0):
        if len(keys) == 0:
            return

        try:
            key = keys.pop()
            value = d[key]

        except KeyError:
            value = StatUnit(key=key)
            d[key] = value

        finally:
            value.update(new_val, process_time_taken, success)
            cls.recursive_update(value.sub, new_val, keys, success, process_time_taken)

    @classmethod
    def update_stats(cls, endpoint, status, time_taken, server_type, success=True, process_time_taken=0):

        cls._stats.update(val=time_taken, process_time_taken=process_time_taken,success=success)
        cls.recursive_update(cls._stats.sub, time_taken, keys=[status, endpoint, server_type], success=success,
            process_time_taken=process_time_taken)

    @classmethod
    def dump_stats(cls):
        return cls._stats.to_dict()

    @classmethod
    def periodic_aggregated_stats_logger(cls):
        hostname = socket.gethostbyname(socket.gethostname())
        service_name = '_'.join(setproctitle.getproctitle().split('_')[1:-1])

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
                    'average_process_time': v['process_time_average'],
                    'total_request_count': v['count'],
                    'success_count': v['success_count']
                })
                for k2, v2 in v['sub'].items():
                    d['CODE_{}'.format(k2)] = v2['count']
                logs.append(d)

        _logger = logging.getLogger('stats')
        for logd in logs:
            _logger.info(dict(logd))
        cls._stats = StatUnit(key='total')
        asyncio.get_event_loop().call_later(300, cls.periodic_aggregated_stats_logger)
