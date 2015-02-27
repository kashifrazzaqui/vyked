import signal
from functools import partial
from collections import defaultdict
import asyncio
from again.utils import unique_hex
from pinger import Pinger


class Registry:
    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
        self._loop = asyncio.get_event_loop()
        self._registered_services = defaultdict(list)
        self._pending_services = defaultdict(list)
        self._client_protocols = {}
        self._service_dependencies = {}
        self._service_protocols = {}
        self._subscription_list = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
        self._pingers = {}

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
        elif request_type == 'subscribe':
            self._handle_subscription(packet)
        elif request_type == 'pong':
            self._handle_pong(packet['node_id'])

    def handle_ping_timeout(self, node):
        print("{} timed out".format(node))

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
                    self._send_activated_packet(self._client_protocols[node],
                                                self._service_dependencies[service_name])
                    nodes.remove(node)

    def _register_service(self, packet:dict, registry_protocol):
        params = packet['params']
        service_name = self._get_full_service_name(params['app'], params["service"], params['version'])
        dependencies = params['vendors']
        service_entry = (params['host'], params['port'], params['node_id'])
        self._registered_services[service_name].append(service_entry)
        self._pending_services[service_name].append(params['node_id'])
        self._client_protocols[params['node_id']] = registry_protocol
        self._connect_to_service(params['host'], params['port'], params['node_id'])
        if self._service_dependencies.get(service_name) is None:
            self._service_dependencies[service_name] = dependencies
        self._handle_pending_registrations()

    @staticmethod
    def _get_full_service_name(app:str, service:str, version:str):
        return "{}/{}/{}".format(app, service, version)

    def _send_activated_packet(self, protocol, dependencies):
        packet = self._make_activated_packet(dependencies)
        protocol.send(packet)
        pass

    def _make_activated_packet(self, vendors):
        vendors_packet = []
        for vendor in vendors:
            vendor_packet = defaultdict(list)
            vendor_name = self._get_full_service_name(vendor['app'], vendor['service'], vendor['version'])
            for host, port, node in self._registered_services[vendor_name]:
                vendor_node_packet = {
                    'host': host,
                    'port': port,
                    'node_id': node
                }
                vendor_packet['name'] = vendor_name
                vendor_packet['addresses'].append(vendor_node_packet)
            vendors_packet.append(vendor_packet)
        params = {
            'vendors': vendors_packet
        }
        packet = {'pid': unique_hex(),
                  'type': 'registered',
                  'params': params}
        return packet

    def _connect_to_service(self, host, port, node_id):
        coro = self._loop.create_connection(self._rfactory, host, port)
        future = asyncio.async(coro)
        future.add_done_callback(partial(self._handle_service_connection, node_id))

    def _handle_service_connection(self, node_id, future):
        transport, protocol = future.result()
        self._service_protocols[node_id] = protocol
        pinger = Pinger(self, self._loop, protocol, node_id)
        self._pingers[node_id] = pinger
        pinger.start_ping()

    def _handle_subscription(self, packet):
        params = packet['params']
        for each in params['subscribe_to']:
            self._subscription_list[each['app']][each['service']][each['version']][each['endpoint']].append(
                (params['ip'], params['port']))

    def _handle_pong(self, node_id):
        pinger = self._pingers[node_id]
        pinger.pong_received()
        pinger.start_ping()


if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
