import asyncio
from functools import partial
import os
import signal

from again.utils import unique_hex
import aiohttp
from aiohttp.web import Application, Response

from vyked.jsonprotocol import ServiceHostProtocol, ServiceClientProtocol
from vyked.registryclient import RegistryClient
from vyked.services import TCPServiceClient, TCPServiceHost, HTTPServiceHost, HTTPServiceClient


class Bus:
    def __init__(self, registry_host:str, registry_port:int):
        self._registry_host = registry_host
        self._registry_port = registry_port
        self._loop = asyncio.get_event_loop()
        self._client_protocols = {}
        self._service_clients = []
        self._pending_requests = []
        self._tcp_host = None
        self._http_host = None
        self._tcp_server = None
        self._http_server = None
        self._host_id = unique_hex()

    def require(self, args):
        for each in args:
            if isinstance(each, TCPServiceClient) or isinstance(each, HTTPServiceClient):
                each.bus = self
                self._service_clients.append(each)

    def serve_tcp(self, service_host:TCPServiceHost):
        self._tcp_host = service_host

    def serve_http(self, service_host:HTTPServiceHost):
        self._http_host = service_host

    def send(self, packet:dict):
        packet['from'] = self._host_id
        func = getattr(self, '_' + packet['type'] + '_sender')
        func(packet)

    def send_http_request(self, app, service, version, method, entity, params):
        path = params.pop('path')
        query_params = params.pop('params', {})
        query_params['app'] = app
        query_params['version'] = version
        query_params['service'] = service
        data = params.pop('data', None)
        headers = params.pop('headers', None)
        cookies = params.pop('cookies', None)
        files = params.pop('files', None)
        auth = params.pop('auth', None)
        allow_redirects = params.pop('allow_redirects', True)
        max_redirects = params.pop('max_redirects', 10)
        encoding = params.pop('encoding', 'utf-8')
        http_version = params.pop('version', aiohttp.HttpVersion11)
        compress = params.pop('compress', None)
        chunked = params.pop('chunked', None)
        expect100 = params.pop('expect100', False)
        connector = params.pop('connector', None)
        loop = params.pop('loop', None)
        read_until_eof = params.pop('read_until_eof', True)
        request_class = params.pop('request_class', None)
        response_class = params.pop('response_class', None)
        host, port, node_id = self._registry_client.resolve(app, service, version, entity)
        # TODO : find a better method create the url
        url = 'http://{}:{}{}'.format(host, port, path)
        response = yield from aiohttp.request(method, url, params=query_params, data=data, headers=headers,
                                              cookies=cookies,
                                              files=files, auth=auth, allow_redirects=allow_redirects,
                                              max_redirects=max_redirects, encoding=encoding, version=http_version,
                                              compress=compress, chunked=chunked, expect100=expect100,
                                              connector=connector,
                                              loop=loop, read_until_eof=read_until_eof, request_class=request_class,
                                              response_class=response_class)
        return response


    def _request_sender(self, packet:dict):
        """
        sends a request to a server from a ServiceClient
        auto dispatch method called from self.send()
        """
        self._pending_requests.append(packet)
        self._clear_request_queue()

    def _publish_sender(self, packet:dict):
        """
        auto dispatch method called from self.send()
        """
        app, service, version, endpoint = packet['app'], packet['service'], packet['version'], packet['endpoint']
        future = self._registry_client.resolve_publication(app, service, version, endpoint)

        def send_publish_packet(publish_packet, f):
            transport, protocol = f.result()
            protocol.send(publish_packet)
            transport.close()

        def fun(fut):
            for node in fut.result():
                packet['to'] = node['node_id']
                coro = self._loop.create_connection(self._host_factory, node['ip'], node['port'])
                connect_future = asyncio.async(coro)
                connect_future.add_done_callback(partial(send_publish_packet, packet))

        future.add_done_callback(fun)

    def host_receive(self, packet:dict, protocol:ServiceHostProtocol):
        if packet['type'] == 'ping':
            self._handle_ping(packet, protocol)
        elif packet['type'] == 'publish':
            client = [sc for sc in self._service_clients if (
                sc.name == packet['service'] and sc.app_name == packet['app'] and sc.version == packet['version'])][0]
            func = getattr(client, packet['endpoint'])
            func(packet['payload'])
        else:
            if self._tcp_host.is_for_me(packet['app'], packet['service'], packet['version']):
                func = getattr(self, '_' + packet['type'] + '_receiver')
                func(packet, protocol)
            else:
                print('wrongly routed packet: ', packet)

    def _request_receiver(self, packet, protocol):
        api_fn = getattr(self._tcp_host, packet['endpoint'])
        if api_fn.is_api:
            from_node_id = packet['from']
            entity = packet['entity']
            result_packet = api_fn(from_id=from_node_id, entity=entity, **packet['payload'])
            protocol.send(result_packet)
        else:
            print('no api found for packet: ', packet)

    def client_receive(self, service_client:TCPServiceClient, packet:dict):
        service_client.process_packet(packet)

    def _stop(self, signame:str):
        print('\ngot signal {} - exiting'.format(signame))
        self._loop.stop()

    def _host_factory(self):
        return ServiceHostProtocol(self)

    def _client_factory(self):
        return ServiceClientProtocol(self)

    def is_tcp_ronin(self):
        return self._tcp_host and not self._tcp_host.ronin

    def is_http_ronin(self):
        return self._http_host and not self._http_host.ronin

    def start(self):
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))

        self._tcp_server = self._create_tcp_service_host()
        self._http_server = self._create_http_service_host()

        if self.is_tcp_ronin() or self.is_http_ronin():
            self._setup_registry_client()

        # TODO: All the ronin conditional logic needs refactor and completion
        if self.is_tcp_ronin():
            tcp_host_ip, tcp_host_port = self._tcp_host.socket_address
            self._registry_client.register_tcp(self._service_clients, tcp_host_ip, tcp_host_port,
                                               *self._tcp_host.properties)

        if self.is_http_ronin():
            ip, port = self._http_host.socket_address
            self._registry_client.register_http(self._service_clients, ip, port,
                                                *self._http_host.properties)

        if self._tcp_server:
            print('Serving TCP on {}'.format(self._tcp_server.sockets[0].getsockname()))
        if self._http_server:
            print('Serving HTTP on {}'.format(self._http_server.sockets[0].getsockname()))
        print("Event loop running forever, press CTRL+c to interrupt.")
        print("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())

        try:
            self._loop.run_forever()
        except Exception as e:
            print(e)
        finally:
            if self._tcp_server:
                self._tcp_server.close()
                self._loop.run_until_complete(self._tcp_server.wait_closed())

            if self._http_server:
                self._http_server.close()
                self._loop.run_until_complete(self._http_server.wait_closed())

            self._loop.close()

    def registration_complete(self):
        if self._tcp_host:
            f = self._create_service_clients()

            def fun(f):
                self._clear_request_queue()

            f.add_done_callback(fun)

    def _create_tcp_service_host(self):
        if self._tcp_host:
            host_ip, host_port = self._tcp_host.socket_address
            host_coro = self._loop.create_server(self._host_factory, host_ip, host_port)
            return self._loop.run_until_complete(host_coro)

    def verify(self, func):
        def verified_func(*args, **kwargs):
            query_dict = args[0].GET
            if self._http_host.is_for_me(query_dict['app'], query_dict['service'], query_dict['version']):
                return func(*args, **kwargs)
            else:
                return Response(body="wrongly routed request".encode())

        return verified_func

    def _create_http_service_host(self):
        if self._http_host:
            host_ip, host_port = self._http_host.socket_address
            app = Application(loop=self._loop)
            routes = self._http_host.get_routes()
            for method, path, handler in routes:
                app.router.add_route(method, path, self.verify(handler))
            if routes:
                handler = app.make_handler()
                http_coro = self._loop.create_server(handler, host_ip, host_port)
                return self._loop.run_until_complete(http_coro)

    def _create_service_clients(self):
        futures = []
        for sc in self._service_clients:
            for host, port, node_id in self._registry_client.get_all_addresses(sc.properties):
                coro = self._loop.create_connection(self._client_factory, host, port)
                future = asyncio.async(coro)
                future.add_done_callback(partial(self._service_client_connection_callback, sc, node_id))
                futures.append(future)
        return asyncio.gather(*futures, return_exceptions=False)

    def _service_client_connection_callback(self, sc, node_id, future):
        transport, protocol = future.result()
        protocol.set_service_client(sc)
        self._client_protocols[node_id] = protocol

    def _setup_registry_client(self):
        self._registry_client = RegistryClient(self._loop, self._registry_host, self._registry_port, self)
        self._registry_client.connect()

    @staticmethod
    def _create_json_service_name(app, service, version):
        return {'app': app, 'service': service, 'version': version}

    def _handle_ping(self, packet, protocol):
        pong_packet = self._make_pong_packet(packet['node_id'], packet['count'])
        protocol.send(pong_packet)

    def _make_pong_packet(self, node_id, count):
        packet = {'type': 'pong', 'node_id': node_id, 'count': count}
        return packet

    def _clear_request_queue(self):
        for packet in self._pending_requests:
            app, service, version, entity = packet['app'], packet['service'], packet['version'], packet['entity']
            node_id = self._registry_client.resolve(app, service, version, entity)
            if node_id is not None:
                client_protocol = self._client_protocols[node_id]
                packet['to'] = node_id
                client_protocol.send(packet)
                self._pending_requests.remove(packet)


if __name__ == '__main__':
    REGISTRY_HOST = '127.0.0.1'
    REGISTRY_PORT = 4500
    HOST_IP = '127.0.0.1'
    HOST_PORT = 8000
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    bus.start()
