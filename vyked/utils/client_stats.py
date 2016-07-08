import logging
import asyncio
import socket
import setproctitle


class ClientStats():
    _client_dict = dict()

    @classmethod
    def update(cls, service_name, host, method, time_taken):
        if not (service_name, host, method) in cls._client_dict.keys():
            cls._client_dict[(service_name, host, method)] = (0, 0)
        count, average = cls._client_dict[(service_name, host, method)]
        count += 1
        average = (average * (count - 1) + time_taken)/count
        cls._client_dict[(service_name, host, method)] = (count, average)

    @classmethod
    def periodic_aggregator(cls):
        hostname = socket.gethostbyname(socket.gethostname())
        service_name = '_'.join(setproctitle.getproctitle().split('_')[1:-1])

        logs = []

        for key, value in cls._client_dict.items():
            d = dict({
                "service_name": service_name,
                "hostname": hostname,
                "client_service": key[0],
                "client_host": key[1],
                "client_method": key[2],
                "average_response_time": int(value[1]),
                "interaction_count": value[0]
            }
            )
            logs.append(d)

        cls._client_dict = dict()

        _logger = logging.getLogger('stats')
        for logd in logs:
            _logger.info(dict(logd))

        asyncio.get_event_loop().call_later(300, cls.periodic_aggregator)
