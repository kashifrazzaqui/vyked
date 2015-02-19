from enum import Enum
import signal
from functools import partial
from collections import defaultdict
import asyncio

from jsonprotocol import RegistryProtocol


class Registry:
    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
        self._loop = asyncio.get_event_loop()
        self._registered_services = defaultdict(list)
        self._pending_services = defaultdict(list)
        self._service_states = {}
        self._service_protocols = {}
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
        if request_type == 'register':
            self._register_service(packet, registry_protocol)

    def _handle_pending_registrations(self):
        for service_name, nodes in self._pending_services:
            depends_on = self._service_dependencies[service_name]
            should_activate = True
            for app, service, version in depends_on:
                if self._registered_services.get(self._get_full_service_name(app, service, version)) is None:
                    should_activate = False
                    break
            for node in nodes:
                if should_activate:
                    self._send_activated_packet(node, self._service_protocols[node])
                    self._pending_services.pop(node)

    def _register_service(self, packet:dict, registry_protocol:RegistryProtocol):
        params = packet['params']
        service_name = self._get_full_service_name(params['app'], params["service"], params['version'])
        dependencies = params['dependencies']
        service_entry = (params['host'], params['port'], params['node_id'])
        self._registered_services[service_name].append(service_entry)
        self._pending_services[service_name].append(params['node_id'])
        self._service_protocols[params['node_id']] = registry_protocol
        if self._service_dependencies[service_name] is None:
            self._service_dependencies[service_name] = dependencies
        self._handle_pending_registrations()


    @staticmethod
    def _get_full_service_name(app:str, service:str, version:str):
        return "{}/{}/{}".format(app, service, version)

    def _send_activated_packet(self, node:str, protocol:RegistryProtocol):
        # Send activated message
        pass


if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
