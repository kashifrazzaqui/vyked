import asyncio
import logging
from functools import partial
import signal
import os

from aiohttp.web import Application

from .bus import TCPBus, PubSubBus
from vyked.registry_client import RegistryClient
from vyked.services import HTTPService, TCPService
from .protocol_factory import get_vyked_protocol
from .utils.log import setup_logging
from vyked.utils.stats import Stats, Aggregator


class Host:
    registry_host = None
    registry_port = None
    pubsub_host = None
    pubsub_port = None
    name = None
    ronin = False
    _host_id = None
    _tcp_service = None
    _http_service = None
    registry_client_ssl = None

    _logger = logging.getLogger(__name__)

    @classmethod
    def _set_process_name(cls):
        from setproctitle import setproctitle

        setproctitle('{}_{}'.format(cls.name, cls._host_id))

    @classmethod
    def _stop(cls, signame: str):
        cls._logger.info('\ngot signal {} - exiting'.format(signame))
        asyncio.get_event_loop().stop()

    @classmethod
    def attach_service(cls, service):
        if isinstance(service, HTTPService):
            cls._http_service = service
        elif isinstance(service, TCPService):
            cls._tcp_service = service
        else:
            cls._logger.error('Invalid argument attached as service')
        cls._set_bus(service)

    @classmethod
    def run(cls):
        if cls._tcp_service or cls._http_service:
            cls._set_host_id()
            cls._setup_logging()

            cls._set_process_name()
            cls._set_signal_handlers()
            cls._start_server()
        else:
            cls._logger.error('No services to host')

    @classmethod
    def _set_signal_handlers(cls):
        asyncio.get_event_loop().add_signal_handler(getattr(signal, 'SIGINT'), partial(cls._stop, 'SIGINT'))
        asyncio.get_event_loop().add_signal_handler(getattr(signal, 'SIGTERM'), partial(cls._stop, 'SIGTERM'))

    @classmethod
    def _create_tcp_server(cls):
        if cls._tcp_service:
            ssl_context = cls._tcp_service.ssl_context
            host_ip, host_port = cls._tcp_service.socket_address
            task = asyncio.get_event_loop().create_server(partial(get_vyked_protocol, cls._tcp_service.tcp_bus),
                                                          host_ip, host_port, ssl=ssl_context)
            result = asyncio.get_event_loop().run_until_complete(task)
            return result

    @classmethod
    def _create_http_server(cls):
        if cls._http_service:
            host_ip, host_port = cls._http_service.socket_address
            ssl_context = cls._http_service.ssl_context
            app = Application(loop=asyncio.get_event_loop())
            fn = getattr(cls._http_service, 'pong')
            app.router.add_route('GET', '/ping', fn)
            app.router.add_route('GET', '/_stats', getattr(cls._http_service, 'stats'))
            for each in cls._http_service.__ordered__:
                fn = getattr(cls._http_service, each)
                if callable(fn) and getattr(fn, 'is_http_method', False):
                    for path in fn.paths:
                        app.router.add_route(fn.method, path, fn)
                        if cls._http_service.cross_domain_allowed:
                            app.router.add_route('options', path, cls._http_service.preflight_response)
            handler = app.make_handler(access_log=cls._logger)
            task = asyncio.get_event_loop().create_server(handler, host_ip, host_port, ssl=ssl_context)
            return asyncio.get_event_loop().run_until_complete(task)

    @classmethod
    def _set_host_id(cls):
        from uuid import uuid4

        cls._host_id = uuid4()

    @classmethod
    def _start_server(cls):
        tcp_server = cls._create_tcp_server()
        http_server = cls._create_http_server()
        cls._register_services()
        cls._create_pubsub_handler()
        cls._subscribe()
        if tcp_server:
            cls._logger.info('Serving TCP on {}'.format(tcp_server.sockets[0].getsockname()))
        if http_server:
            cls._logger.info('Serving HTTP on {}'.format(http_server.sockets[0].getsockname()))
        cls._logger.info("Event loop running forever, press CTRL+c to interrupt.")
        cls._logger.info("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())
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

    @classmethod
    def _create_pubsub_handler(cls):
        if not cls.ronin:
            if cls._tcp_service:
                asyncio.get_event_loop().run_until_complete(
                    cls._tcp_service.pubsub_bus
                    .create_pubsub_handler(cls.pubsub_host, cls.pubsub_port))
            if cls._http_service:
                asyncio.get_event_loop().run_until_complete(
                    cls._http_service.pubsub_bus.create_pubsub_handler(cls.pubsub_host, cls.pubsub_port))

    @classmethod
    def _subscribe(cls):
        if not cls.ronin:
            if cls._tcp_service:
                asyncio.async(
                    cls._tcp_service.pubsub_bus.register_for_subscription(cls._tcp_service.host, cls._tcp_service.port,
                                                                          cls._tcp_service.node_id,
                                                                          cls._tcp_service.clients))
            if cls._http_service:
                asyncio.async(
                    cls._http_service.pubsub_bus.register_for_subscription(cls._http_service.host,
                                                                           cls._http_service.port,
                                                                           cls._http_service.node_id,
                                                                           cls._http_service.clients))

    @classmethod
    def _set_bus(cls, service):
        registry_client = RegistryClient(
            asyncio.get_event_loop(), cls.registry_host, cls.registry_port, cls.registry_client_ssl)
        tcp_bus = TCPBus(registry_client)
        registry_client.conn_handler = tcp_bus
        # pubsub_bus = PubSubBus(registry_client, ssl_context=cls._tcp_service._ssl_context)
        pubsub_bus = PubSubBus(registry_client)  # , cls._tcp_service._ssl_context)

        registry_client.bus = tcp_bus
        if isinstance(service, TCPService):
            tcp_bus.tcp_host = service
        if isinstance(service, HTTPService):
            tcp_bus.http_host = service
        service.tcp_bus = tcp_bus
        service.pubsub_bus = pubsub_bus

    @classmethod
    def _register_services(cls):
        if not cls.ronin:
            if cls._tcp_service:
                cls._tcp_service.register()
            if cls._http_service:
                cls._http_service.register()

    @classmethod
    def _setup_logging(cls):
        host = cls._tcp_service if cls._tcp_service else cls._http_service
        identifier = '{}_{}'.format(host.name, host.socket_address[1])
        setup_logging(identifier)
        Stats.service_name = host.name
        Stats.periodic_stats_logger()
        Aggregator.periodic_aggregated_stats_logger()
