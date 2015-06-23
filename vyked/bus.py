import asyncio
from functools import partial
import os
import logging

from again.utils import unique_hex
from retrial.retrial import retry
import aiohttp
from aiohttp.web import Application, Response

import signal
from .jsonprotocol import ServiceHostProtocol, ServiceClientProtocol
from .registryclient import RegistryClient
from .services import TCPServiceClient, HTTPServiceClient, HTTPApplicationService
from .pinger import Pinger

HTTP = 'http'
TCP = 'tcp'

PING_TIMEOUT = 10

logger = logging.getLogger(__name__)

def _retry_for(result):
    if isinstance(result, tuple):
        return not isinstance(result[0], asyncio.transports.Transport) or not isinstance(result[1],
                                                                                         asyncio.Protocol)
    return True

class Bus:
    def __init__(self):

        self._registry_client = None

        self._client_protocols = {}
        self._pingers = {}
        self._service_clients = []
        self._node_clients = {}

        self._pending_requests = []
        self._unacked_publish = {}

        self._tcp_host = None
        self._http_host = None
        self._host_id = unique_hex()

    def require(self, args):
        for each in args:
            if isinstance(each, (TCPServiceClient, HTTPServiceClient)):
                each.bus = self
                self._service_clients.append(each)

    def serve_tcp(self, service_host):
        self._tcp_host = service_host
        self._tcp_host.bus = self

    def serve_http(self, service_host):
        self._http_host = service_host
        self._http_host.bus = self

    def send(self, packet:dict):
        packet['from'] = self._host_id
        func = getattr(self, '_' + packet['type'] + '_sender')
        func(packet)

    def send_http_request(self, app:str, service:str, version:str, method:str, entity:str, params:dict):
        """
        a convenience method that allows you to send a well formatted http request to another service
        """
        host, port, node_id, service_type = self._registry_client.resolve(service, version, entity, HTTP)

        url = 'http://{}:{}{}'.format(host, port, params.pop('path'))

        http_keys = ['data', 'headers', 'cookies', 'auth', 'allow_redirects', 'compress', 'chunked']
        kwargs = {k: params[k] for k in http_keys if k in params}

        query_params = params.pop('params', {})

        if app is not None:
            query_params['app'] = app

        query_params['version'] = version
        query_params['service'] = service

        response = yield from aiohttp.request(method, url, params=query_params, **kwargs)
        return response

    def _request_sender(self, packet: dict):
        """
        sends a request to a server from a ServiceClient
        auto dispatch method called from self.send()
        """
        self._pending_requests.append(packet)
        self._clear_request_queue()

    def _message_sub_sender(self, packet: dict):
        packet['ip'], packet['port'] = self._tcp_host.socket_address
        self._registry_client.subscribe_for_message(packet)

    def _publish(self, future, packet):
        def send_publish_packet(publish_packet, f):
            transport, protocol = f.result()
            protocol.send(publish_packet)
            transport.close()

        def fun(fut):
            for node in fut.result():
                packet['to'] = node['node_id']
                pid = unique_hex()
                packet['pid'] = pid
                self._unacked_publish[pid] = packet
                coro = asyncio.get_event_loop().create_connection(self._host_factory, node['ip'], node['port'])
                connect_future = asyncio.async(coro)
                connect_future.add_done_callback(partial(send_publish_packet, packet))

        future.add_done_callback(fun)

    def _publish_sender(self, packet: dict):
        """
        auto dispatch method called from self.send()
        """
        service, version, endpoint = packet['service'], packet['version'], packet['endpoint']
        future = self._registry_client.resolve_publication(service, version, endpoint)
        self._publish(future, packet)

    def _message_pub_sender(self, packet: dict):
        app, service, version, endpoint, entity = packet['app'], packet['service'], packet['version'], packet[
            'endpoint'], packet['entity']
        future = self._registry_client.resolve_message_publication(service, version, endpoint, entity)
        self._publish(future, packet)

    def host_receive(self, packet: dict, protocol: ServiceHostProtocol):
        if packet['type'] == 'ping':
            self._handle_ping(packet, protocol)
        elif packet['type'] == 'pong':
            self._handle_pong(packet['node_id'], packet['count'])
        elif packet['type'] == 'ack':
            pid = packet['pid']
            self._unacked_publish.pop(pid)
        elif packet['type'] == 'publish':
            client = \
                [sc for sc in self._service_clients if (sc.name == packet['service'] and sc.version == packet['version'])][
                    0]
            func = getattr(client, packet['endpoint'])
            asyncio.async(func(packet['payload']))
            self.send_ack(protocol, packet['pid'])
        else:
            if self._tcp_host.is_for_me(packet['service'], packet['version']):
                func = getattr(self, '_' + packet['type'] + '_receiver')
                func(packet, protocol)
            else:
                logger.warn('wrongly routed packet: ', packet)

    def _request_receiver(self, packet, protocol):
        api_fn = getattr(self._tcp_host, packet['endpoint'])
        if api_fn.is_api:
            from_node_id = packet['from']
            entity = packet['entity']
            future = asyncio.async(api_fn(from_id=from_node_id, entity=entity, **packet['payload']))

            def send_result(f):
                result_packet = f.result()
                protocol.send(result_packet)

            future.add_done_callback(send_result)
        else:
            print('no api found for packet: ', packet)

    def client_receive(self, service_client:TCPServiceClient, packet:dict):
        if packet['type'] == 'ping':
            self._handle_pong(packet['node_id'], packet['count'])
        elif packet['type'] == 'pong':
            pinger = self._pingers[packet['node_id']]
            asyncio.async(pinger.pong_received(packet['count']))
        else:
            service_client.process_packet(packet)

    def _stop(self, signame:str):
        print('\ngot signal {} - exiting'.format(signame))
        asyncio.get_event_loop().stop()

    def _host_factory(self):
        return ServiceHostProtocol(self)

    def _client_factory(self):
        return ServiceClientProtocol(self)

    def is_tcp_ronin(self):
        return self._tcp_host and not self._tcp_host.ronin

    def is_http_ronin(self):
        return self._http_host and not self._http_host.ronin

    def start(self, registry_host: str, registry_port: int):
        self._set_process_name()
        asyncio.get_event_loop().add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        asyncio.get_event_loop().add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))

        tcp_server = self._create_tcp_service_host()
        http_server = self._create_http_service_host()
        if self.is_tcp_ronin() or self.is_http_ronin():
            self._setup_registry_client(registry_host, registry_port)

        # TODO: All the ronin conditional logic needs refactor and completion
        if self.is_tcp_ronin():
            tcp_host_ip, tcp_host_port = self._tcp_host.socket_address
            self._registry_client.register_tcp(self._service_clients, tcp_host_ip, tcp_host_port,
                                               *self._tcp_host.properties)

        if self.is_http_ronin():
            ip, port = self._http_host.socket_address
            self._registry_client.register_http(self._service_clients, ip, port, *self._http_host.properties)

        if tcp_server:
            logger.info('Serving TCP on {}'.format(tcp_server.sockets[0].getsockname()))
        if http_server:
            logger.info('Serving HTTP on {}'.format(http_server.sockets[0].getsockname()))
        logger.info("Event loop running forever, press CTRL+c to interrupt.")
        logger.info("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())

        try:
            asyncio.get_event_loop().run_forever()
        except Exception as e:
            print(e)
        finally:
            if tcp_server:
                tcp_server.close()
                asyncio.get_event_loop().run_until_complete(tcp_server.wait_closed())

            if http_server:
                http_server.close()
                asyncio.get_event_loop().run_until_complete(http_server.wait_closed())

            asyncio.get_event_loop().close()

    def registration_complete(self):
        f = self._create_service_clients()

        def fun(fut):
            if self._tcp_host:
                self._clear_request_queue()

        f.add_done_callback(fun)

    def _create_tcp_service_host(self):
        if self._tcp_host:
            host_ip, host_port = self._tcp_host.socket_address
            host_coro = asyncio.get_event_loop().create_server(self._host_factory, host_ip, host_port)
            return asyncio.get_event_loop().run_until_complete(host_coro)

    def _verify_service_and_version(self, func):
        def verified_func(*args, **kwargs):
            query_dict = args[0].GET
            if isinstance(self._http_host, HTTPApplicationService):
                return func(*args, **kwargs)
            if 'service' in query_dict and 'version' in query_dict:
                if self._http_host.is_for_me(query_dict['service'], query_dict['version']):
                    return func(*args, **kwargs)
                else:
                    return Response(status=421, body="421 wrongly routed request".encode())
            else:
                return Response(status=400, body="400 bad request".encode())

        return verified_func

    def _get_preflight_response(self, request):
        return Response(status=200,
                        headers={'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,PUT',
                                 'Access-Control-Allow-Headers': 'accept, content-type, token'})

    def _create_http_service_host(self):
        if self._http_host:
            host_ip, host_port = self._http_host.socket_address
            ssl_context = self._http_host.ssl_context
            app = Application(loop=asyncio.get_event_loop())
            for each in self._http_host.__ordered__:
                fn = getattr(self._http_host, each)
                if callable(fn) and getattr(fn, 'is_http_method', False):
                    for path in fn.paths:
                        app.router.add_route(fn.method, path, self._verify_service_and_version(fn))
                        if self._http_host.cross_domain_allowed:
                            app.router.add_route('options', path, self._get_preflight_response)
            fn = getattr(self._http_host, 'pong')
            app.router.add_route('GET', '/ping', fn)
            handler = app.make_handler()
            http_coro = asyncio.get_event_loop().create_server(handler, host_ip, host_port, ssl=ssl_context)
            return asyncio.get_event_loop().run_until_complete(http_coro)

    def _create_service_clients(self):
        futures = []
        for sc in self._service_clients:
            for host, port, node_id, service_type in self._registry_client.get_all_addresses(sc.properties):
                self._node_clients[node_id] = sc
                future = self._connect_to_client(host, node_id, port, service_type)
                futures.append(future)
        return asyncio.gather(*futures, return_exceptions=False)

    @retry(should_retry_for_result=_retry_for, timeout=10)
    def _connect_to_client(self, host, node_id, port, service_type):
        future = asyncio.async(asyncio.get_event_loop().create_connection(self._client_factory, host, port))
        future.add_done_callback(
            partial(self._service_client_connection_callback, self._node_clients[node_id], node_id, service_type))
        return future

    def _service_client_connection_callback(self, sc, node_id, service_type, future):
        transport, protocol = future.result()
        protocol.set_service_client(sc)
        if service_type == TCP:
            pinger = Pinger(self, asyncio.get_event_loop())
            self._pingers[node_id] = pinger
            pinger.register_tcp_service(protocol, node_id)
            asyncio.async(pinger.start_ping())
        self._client_protocols[node_id] = protocol

    def _setup_registry_client(self, host: str, port: int):
        self._registry_client = RegistryClient(asyncio.get_event_loop(), host, port, self)
        self._registry_client.connect()

    @staticmethod
    def _create_json_service_name(app, service, version):
        return {'app': app, 'service': service, 'version': version}

    def _handle_ping(self, packet, protocol):
        pong_packet = self._make_pong_packet(packet['node_id'], packet['count'])
        protocol.send(pong_packet)

    def _handle_pong(self, node_id, count):
        pinger = self._pingers[node_id]
        asyncio.async(pinger.pong_received(count))

    def _make_pong_packet(self, node_id, count):
        packet = {'type': 'pong', 'node_id': node_id, 'count': count}
        return packet

    def _clear_request_queue(self):
        for packet in self._pending_requests:
            app, service, version, entity = packet['app'], packet['service'], packet['version'], packet['entity']
            node = self._registry_client.resolve(service, version, entity, TCP)
            if node is not None:
                node_id = node[2]
                client_protocol = self._client_protocols[node_id]
                packet['to'] = node_id
                client_protocol.send(packet)
                self._pending_requests.remove(packet)

    @staticmethod
    def send_ack(protocol, pid):
        packet = {'type': 'ack', 'pid': pid}
        protocol.send(packet)

    def handle_ping_timeout(self, node_id):
        print("Service client connection timed out".format(node_id))
        self._pingers.pop(node_id, None)
        service_props = self._registry_client.get_for_node(node_id)
        print(service_props)
        if service_props is not None:
            asyncio.async(self._connect_to_client(*service_props))

    def _set_process_name(self):
        from setproctitle import setproctitle

        if self._tcp_host:
            setproctitle('{}_{}_{}'.format(self._tcp_host.name, self._tcp_host.version, self._host_id))
        elif self._http_host:
            setproctitle('{}_{}_{}'.format(self._http_host.name, self._http_host.version, self._host_id))


if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    HOST_IP = '127.0.0.1'
    HOST_PORT = 8000
    bus = Bus()
    bus.start(REGISTRY_HOST, REGISTRY_PORT)
