import signal
import asyncio
from functools import partial
from collections import defaultdict

from again.utils import unique_hex

from .pinger import Pinger


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
        self._dead_service_listeners = defaultdict(list)
        self._subscription_list = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self._message_sub_list = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
        self._pingers = {}

    def _rfactory(self):
        from vyked.jsonprotocol import RegistryProtocol

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

    def receive(self, packet:dict, registry_protocol, transport):
        request_type = packet['type']
        if request_type == 'register':
            self._register_service(packet, registry_protocol, *transport.get_extra_info('peername'))
        elif request_type == 'subscribe':
            self._handle_subscription(packet, *transport.get_extra_info('peername'))
        elif request_type == 'message_sub':
            self._subscribe_for_message(packet, *transport.get_extra_info('peername'))
        elif request_type == 'pong':
            self._handle_pong(packet['node_id'], packet['count'])
        elif request_type == 'resolve_publication':
            self._resolve_publication(packet, registry_protocol)
        elif request_type == 'resolve_message_publication':
            self._resolve_message_publication(packet, registry_protocol)
        elif request_type == 'service_death_sub':
            self._add_service_death_listener(packet)
        elif request_type == 'get_instances':
            self._get_service_instances(packet, registry_protocol)

    def deregister_service(self, node_id):
        for service, nodes in self._registered_services.items():
            service_present = False
            for node in nodes:
                if node[2] == node_id:
                    nodes.remove(node)
                    service_present = True
            if service_present:
                self._notify_consumers(service, node_id)
                self._remove_service_death_listener(node_id)
                self._notify_service_death_listeners(service, node_id)
                if not len(nodes):
                    for consumer in self._get_consumers(service):
                        self._pending_services[consumer] = [node_id for host, port, node_id, service_type in
                                                            self._registered_services[consumer]]

        self._service_protocols.pop(node_id, None)
        self._client_protocols.pop(node_id, None)

    def handle_ping_timeout(self, node_id):
        print("{} timed out".format(node_id))
        self.deregister_service(node_id)

    def _handle_pending_registrations(self):
        for service_name in self._pending_services:
            depends_on = self._service_dependencies[service_name]
            should_activate = True
            for dependency in depends_on:
                if self._registered_services.get(
                        self._get_full_service_name(dependency["service"], dependency["version"])) is None:
                    should_activate = False
                    break
            nodes = self._pending_services[service_name]
            for node in nodes:
                if should_activate:
                    self._send_activated_packet(self._client_protocols[node],
                                                self._service_dependencies[service_name])
                    nodes.remove(node)

    def _register_service(self, packet:dict, registry_protocol, host, port):
        params = packet['params']
        service_name = self._get_full_service_name(params["service"], params['version'])
        dependencies = params['vendors']
        service_entry = (host, params['port'], params['node_id'], params['type'])
        self._registered_services[service_name].append(service_entry)
        self._pending_services[service_name].append(params['node_id'])
        self._client_protocols[params['node_id']] = registry_protocol
        self._connect_to_service(host, params['port'], params['node_id'], params['type'])
        if self._service_dependencies.get(service_name) is None:
            self._service_dependencies[service_name] = dependencies
        self._handle_pending_registrations()

    @staticmethod
    def _get_full_service_name(service: str, version):
        return "{}/{}".format(service, version)

    def _send_activated_packet(self, protocol, dependencies):
        packet = self._make_activated_packet(dependencies)
        protocol.send(packet)
        pass

    def _make_activated_packet(self, vendors):
        vendors_packet = []
        for vendor in vendors:
            vendor_packet = defaultdict(list)
            vendor_name = self._get_full_service_name(vendor['service'], vendor['version'])
            for host, port, node, service_type in self._registered_services[vendor_name]:
                vendor_node_packet = {
                    'host': host,
                    'port': port,
                    'node_id': node,
                    'type': service_type
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

    def _connect_to_service(self, host, port, node_id, service_type):
        if service_type == 'tcp':
            coro = self._loop.create_connection(self._rfactory, host, port)
            future = asyncio.async(coro)
            future.add_done_callback(partial(self._handle_service_connection, node_id))
        else:
            pinger = Pinger(registry, self._loop)
            pinger.register_http_service(host, port, node_id)
            self._pingers[node_id] = pinger
            asyncio.async(pinger.start_ping())

    def _handle_service_connection(self, node_id, future):
        transport, protocol = future.result()
        self._service_protocols[node_id] = protocol
        pinger = Pinger(registry, self._loop)
        pinger.register_tcp_service(protocol, node_id)
        self._pingers[node_id] = pinger
        asyncio.async(pinger.start_ping())

    def _handle_subscription(self, packet, host, port):
        params = packet['params']
        for each in params['subscribe_to']:
            self._subscription_list[each['service']][each['version']][each['endpoint']].append(
                (host, params['port'], params['node_id']))

    def _handle_pong(self, node_id, count):
        pinger = self._pingers[node_id]
        pinger.pong_received(count)
        asyncio.async(pinger.start_ping())

    def _resolve_publication(self, packet, protocol):
        params = packet['params']
        service, version, endpoint = params['service'], params['version'], params['endpoint']
        nodes = [{'ip': ip, 'port': port, 'node_id': node} for ip, port, node in
                 self._subscription_list[service][version][endpoint]]
        result = {'type': 'subscription_list', 'request_id': packet['request_id'], 'nodes': nodes}
        protocol.send(result)

    def _resolve_message_publication(self, packet, protocol):
        params = packet['params']
        service, version, endpoint, entity = params['service'], params['version'], params['endpoint'], params['entity']
        nodes = [{'ip': ip, 'port': port, 'node_id': node} for ip, port, node in
                 self._message_sub_list[service][version][endpoint][entity]]
        result = {'type': 'message_subscription_list', 'request_id': packet['request_id'], 'nodes': nodes}
        protocol.send(result)

    def _get_consumers(self, vendor):
        consumers = []
        for service, vendors in self._service_dependencies.items():
            for each in vendors:
                vendor_name = self._get_full_service_name(each['service'], each['version'])
                if vendor == vendor_name:
                    consumers.append(service)
        return consumers

    def _notify_consumers(self, vendor, node_id):
        packet = self._make_deregister_packet(node_id, vendor)
        for consumer in self._get_consumers(vendor):
            for host, port, node, service_type in self._registered_services[consumer]:
                protocol = self._client_protocols[node]
                protocol.send(packet)

    def _make_deregister_packet(self, node_id, vendor):
        packet = {'type': 'deregister'}
        params = {'node_id': node_id, 'vendor': vendor}
        packet['params'] = params
        return packet

    def _subscribe_for_message(self, packet, host, port):
        service, version, endpoint, entity = packet['service'], packet['version'], packet[
            'endpoint'], packet['entity']
        ip, port, node_id = packet['ip'], packet['port'], packet['node_id']
        self._message_sub_list[service][version][endpoint][entity].append((host, port, node_id))

    def _add_service_death_listener(self, packet):
        service, version, node = packet['service'], packet['version'], packet['node']
        params = packet['params']
        service_name = self._get_full_service_name(params['service'], params['version'])
        self._dead_service_listeners[service_name].append(node)

    def _remove_service_death_listener(self, node_id):
        for service, listeners in self._dead_service_listeners.items():
            if node_id in listeners:
                listeners.remove()

    def _notify_service_death_listeners(self, service, node_id):
        for node in self._dead_service_listeners[service]:
            protocol = self._client_protocols[node]
            protocol.send(self._make_service_dead_packet(service, node_id))

    @staticmethod
    def _make_service_dead_packet(service, node_id):
        params = {'node_id': node_id, 'service': service.split('/')[0], 'version': service.split('/')[1]}
        packet = {'type': 'service_dead', 'params': params}
        return packet

    def _get_service_instances(self, packet, registry_protocol):
        params = packet['params']
        service, version = params['service'], params['version']
        service_name = self._get_full_service_name(service, version)
        instances = []
        if service_name in self._registered_services:
            instances = [{'host': host, 'port': port, 'node': node, 'type': service_type} for host, port, node, service_type in
                         self._registered_services[service_name]]
        instance_packet_params = {'service': service, 'version': version, 'instances': instances}
        instance_packet = {'type': 'instances', 'params': instance_packet_params}
        registry_protocol.send(instance_packet)


if __name__ == '__main__':
    from setproctitle import setproctitle

    setproctitle("registry")
    REGISTRY_HOST = None
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
