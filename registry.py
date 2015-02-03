from enum import Enum
import signal
from functools import partial
import asyncio
from again.utils import unique_hex

from jsonprotocol import RegistryProtocol


class Registry:
    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
        self._loop = asyncio.get_event_loop()
        self._services = {}
        self._service_states = {}
        self._service_dependencies = {}

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
        request_type = packet['type']
        if request_type == 'host':
            self._host_service(packet)
        elif request_type == 'provision':
            self._handle_provision(packet, registry_protocol)
        elif request_type == 'register':
            self._handle_registration(packet['params'])

    def _host_service(self, packet:dict):
        params = packet['params']
        service_name = self._get_full_service_name(params['app'], params["service"], params['version'])
        self._service_states[service_name] = self._ServiceState.HOSTED
        service_entry = (params['host'], params['port'], params['node_id'])
        if self._services.get(service_name) is None:
            self._services[service_name] = [service_entry]
        else:
            self._services[service_name].append(service_entry)

    def _handle_registration(self, params:dict):
        full_service_name = self._get_full_service_name(params['app'], params['service'], params['version'])
        self._service_states[full_service_name] = self._ServiceState.REGISTERED
        # TODO : Check if provisioning is successful for a service

    @staticmethod
    def _get_full_service_name(app:str, service:str, version:str):
        return "{}/{}/{}".format(app, service, version)

    def _handle_provision(self, packet:dict, registry_protocol:RegistryProtocol):
        params = packet['params']
        result = {}
        self._add_dependencies(params['app'], params['service'], params['version'], params['service_names'])
        for app_name, service_name, version in params['service_names']:
            key = self._get_full_service_name(app_name, service_name, version)
            result[key] = self._services.get(key)
        result_params = {'result': result,
                         'request_id': params['request_id']}
        result_packet = {'pid': unique_hex(),
                         'type': 'provision',
                         'params': result_params}
        registry_protocol.send(result_packet)

    def _add_dependencies(self, app, service, version, dependencies):
        full_service_name = self._get_full_service_name(app, service, version)
        for dependency in dependencies:
            if self._service_dependencies.get(full_service_name) is None:
                self._service_dependencies[full_service_name] = [dependency]
            else:
                if dependency not in self._service_dependencies[full_service_name]:
                    self._service_dependencies[full_service_name].append(dependency)

    class _ServiceState(Enum):
        HOSTED = 'hosted'
        REGISTERED = 'registered'
        ACTIVATION_REQUESTED = 'activation_requested'
        ACTIVATED = 'activated'

if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
