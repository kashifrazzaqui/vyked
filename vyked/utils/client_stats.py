import logging
import asyncio
import socket
import setproctitle

class ClientStats():
    _client_dict = dict()

    @classmethod
    def update(cls, service_name, host):
        if not (service_name, host) in cls._client_dict.keys():
            cls._client_dict[(service_name, host)] = 0
        cls._client_dict[(service_name, host)] += 1

    @classmethod
    def periodic_aggregator(cls):
        hostname = socket.gethostbyname(socket.gethostname())
        service_name = '_'.join(setproctitle.getproctitle().split('_')[:-1])

        logs = []

        for key, value in cls._client_dict.items():
            d = dict({
                "service_name" : service_name,
                "hostname" : hostname,
                "client_service" : key[0],
                "client_host" : key[1],
                "interaction_count" : value
            }
            )
            logs.append(d)

        _logger = logging.getLogger('stats')
        for logd in logs:
            _logger.info(dict(logd))

        asyncio.get_event_loop().call_later(300, cls.periodic_aggregator)
