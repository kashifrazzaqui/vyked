import signal
from functools import partial
import asyncio

from jsonprotocol import RegistryProtocol

class Registry:
    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
        self._loop = asyncio.get_event_loop()
        self._services = {}

    def _rfactory(self):
        return RegistryProtocol(self)

    def start(self):
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))
        registry_coro = self._loop.create_server(self._rfactory, self._ip, self._port)
        self._server = self._loop.run_until_complete(registry_coro)

    def _stop(self, signame:str):
        print('\ngot signal {} - exiting'.format(signame))
        self._loop.stop()

    def receive(self, packet:dict, registry_protocol:RegistryProtocol):
        request_type = packet["type"]
        if request_type == "host":
            self._host_service(packet)
        elif request_type == "resolve":
            pass

    def _host_service(self, packet):
        params = packet["params"]
        service_name = self._get_full_service_name(params['app'], params["service"], params['version'])
        service_entry = {'host': params['host'], 'port': params['port'], 'node_id': params['node_id']}
        if self._services.get(service_name) is None:
            self._services[service_name] = [service_entry]
        else:
            self._services[service_name].append(service_entry)


    @staticmethod
    def _get_full_service_name(app, service, version):
        return "{}/{}/{}".format(app, service, version)

if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
