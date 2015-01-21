import uuid
import asyncio
from functools import partial
import os
import signal
from jsonprotocol import StreamingJSONServerProtocol
from registryclient import RegistryClient
from services import ServiceClient, ServiceHost


class Bus:
    _ASTERISK = '*'

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._pending = {}
        self._client_routing_table = {}
        self._service_clients = []

    def require(self, *args):
        for each in args:
            if isinstance(each, ServiceClient):
                each.set_bus(self)
                self._service_clients.append(each)

    def serve(self, service_host, ip_addr, port):
        self._host = service_host
        self._host_id = uuid.uuid4()

    def send(self, packet):
        # TODO: attach a sender node id for getting responses
        func = getattr(self, '_' + packet['type'] + '_sender')
        func(packet)

    def _request_sender(self, packet):
        """
        sends a request to a server from a ServiceClient
        auto dispatch method called from self.send()
        """
        entity = packet['entity']
        if entity == Bus._ASTERISK:
            pass  # TODO: Send to each instance of this service


    def _message_sender(self, packet):
        """
        auto dispatch method called from self.send()
        """
        pass

    def register_client(self, client_id, client):
        self._client_routing_table[client_id] = client

    def receive(self, packet):
        # receives data from service clients
        pass

    def _stop(self, signame):
        print('\ngot signal {} - exiting'.format(signame))
        self._loop.stop()

    def _make_host(self):
        self._host_transport = StreamingJSONServerProtocol(self)
        return self._host_transport

    def start(self):
        self._loop.add_signal_handler(getattr(signal, 'SIGINT'), partial(self._stop, 'SIGINT'))
        self._loop.add_signal_handler(getattr(signal, 'SIGTERM'), partial(self._stop, 'SIGTERM'))

        self._create_service_hosts()
        self._setup_registry_client()
        # TODO: ask it for endpoints for various service clients
        self._create_service_clients()  # make tcp connections to all required services
        # TODO: activate service host with registry

        print('Serving on {}'.format(self._tcp_server.sockets[0].getsockname()))
        print("Event loop running forever, press CTRL+c to interrupt.")
        print("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())

        try:
            self._loop.run_forever()
        except Exception as e:
            print(e)
        finally:
            self._tcp_server.close()
            self._loop.run_until_complete(self._tcp_server.wait_closed())
            self._loop.close()


    def _create_service_hosts(self):
        host_coro = self._loop.create_server(self._make_host, '127.0.0.1', 8000)
        task = asyncio.async(host_coro)
        self._tcp_server = self._loop.run_until_complete(task)

    def _create_service_clients(self):
        # ask
        # transport, protocol = asyncio.create_connection()
        # save reference to each protocol in an endpoint:protocol map
        # and use its 'send' method to send data to this service instance
        # any data received in this protocol will be sent to bus.receive
        pass

    def _setup_registry_client(self):
        self._registry = RegistryClient()
        self._registry.connect()
        service_names = ["/{}/{}".format(client.app_name, client.name) for client in self._service_clients]
        self._loop.run_until_complete(self._registry.provision(service_names))
        host, port = self._registry.get_endpoint()
        # TODO: create asyncio.create_connection using self._make_registry_protocol
        # TODO: run until complete
        self._registry.add(self._host)


if __name__ == '__main__':
    bus = Bus()  # TODO: Needs service registry host, port in constructor
    bus.start()
