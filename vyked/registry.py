import signal
import asyncio
from functools import partial
from collections import defaultdict

from .pinger import Pinger
from .packet import ControlPacket


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
        elif request_type == 'pong':
            self._handle_pong(packet['node_id'], packet['count'])
        elif request_type == 'get_instances':
            self._get_service_instances(packet, registry_protocol)

    def deregister_service(self, node_id):
        for service, nodes in self._registered_services.items():
            service_present = False
            for node in nodes:
                if node[2] == node_id:
                    nodes.remove(node)
                    service_present = True
                    self._service_protocols.pop(node_id, None)
                    self._client_protocols.pop(node_id, None)
            if service_present:
                self._notify_consumers(service, node_id)
                if not len(nodes):
                    for consumer in self._get_consumers(service):
                        self._pending_services[consumer] = [node_id for host, port, node_id, service_type in
                                                            self._registered_services[consumer]]

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
        vendor_names = [self._get_full_service_name(vendor['service'], vendor['version']) for vendor in vendors]
        return ControlPacket.activated(vendor_names, self._registered_services)

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

    def _handle_pong(self, node_id, count):
        pinger = self._pingers[node_id]
        asyncio.async(pinger.pong_received(count))

    def _get_consumers(self, vendor):
        consumers = []
        for service, vendors in self._service_dependencies.items():
            for each in vendors:
                vendor_name = self._get_full_service_name(each['service'], each['version'])
                if vendor == vendor_name:
                    consumers.append(service)
        return consumers

    def _notify_consumers(self, vendor, node_id):
        packet = ControlPacket.deregister(node_id, vendor)
        for consumer in self._get_consumers(vendor):
            for host, port, node, service_type in self._registered_services[consumer]:
                protocol = self._client_protocols[node]
                protocol.send(packet)

    def _get_service_instances(self, packet, registry_protocol):
        params = packet['params']
        service, version = params['service'], params['version']
        service_name = self._get_full_service_name(service, version)
        instance_packet = ControlPacket.send_instances(service, version, self._registered_services[
            service_name]) if service_name in self._registered_services else ControlPacket.send_instances(service,
                                                                                                          version, [])
        registry_protocol.send(instance_packet)


if __name__ == '__main__':
    from setproctitle import setproctitle

    setproctitle("registry")
    REGISTRY_HOST = None
    REGISTRY_PORT = 4500
    registry = Registry(REGISTRY_HOST, REGISTRY_PORT)
    registry.start()
