import signal
from functools import partial
from collections import defaultdict
import asyncio
from again.utils import unique_hex


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
        from jsonprotocol import RegistryProtocol
        return RegistryProtocol(self)

    def start(self):
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))
        registry_coro = self._loop.create_server(self._rfactory, self._ip, self._port)
        self._server = self._loop.run_until_complete(registry_coro)
        try:
            self._loop.run_forever()
        except Exception as e:
            print(e)
        finally:
            self._server.close()
            self._loop.run_until_complete(self._server.wait_closed())
            self._loop.close()


    def _stop(self, signame:str):
        print('\ngot signal {} - exiting'.format(signame))
        self._loop.stop()

    def receive(self, packet:dict, registry_protocol):
        request_type = packet['type']
        if request_type == 'register':
            self._register_service(packet, registry_protocol)

    def _handle_pending_registrations(self):
        for service_name in self._pending_services:
            depends_on = self._service_dependencies[service_name]
            should_activate = True
            for dependency in depends_on:
                if self._registered_services.get(self._get_full_service_name(dependency["app"], dependency["service"],
                                                                             dependency["version"])) is None:
                    should_activate = False
                    break
            nodes = self._pending_services[service_name]
            for node in nodes:
                if should_activate:
                    self._send_activated_packet(node, self._service_protocols[node])
                    nodes.remove(node)

    def _register_service(self, packet:dict, registry_protocol):
        params = packet['params']
        service_name = self._get_full_service_name(params['app'], params["service"], params['version'])
        dependencies = params['dependencies']
        service_entry = (params['host'], params['port'], params['node_id'])
        self._registered_services[service_name].append(service_entry)
        self._pending_services[service_name].append(params['node_id'])
        self._service_protocols[params['node_id']] = registry_protocol
        if self._service_dependencies.get(service_name) is None:
            self._service_dependencies[service_name] = dependencies
        self._handle_pending_registrations()


    @staticmethod
    def _get_full_service_name(app:str, service:str, version:str):
        return "{}/{}/{}".format(app, service, version)

    def _send_activated_packet(self, node:str, protocol):
        packet = self._make_activated_packet()
        protocol.send(packet)
        pass

    @staticmethod
    def _make_activated_packet():
        packet = {'pid': unique_hex(),
                  'type': 'registered'}
        return packet

if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
