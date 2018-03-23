import asyncio
import logging
from functools import partial
import signal
import os
import warnings

from aiohttp.web import Application

from .bus import TCPBus, PubSubBus
from vyked.registry_client import RegistryClient
from vyked.services import HTTPService, TCPService
from .protocol_factory import get_vyked_protocol
from .utils.log import setup_logging
from vyked.utils.decorators import deprecated
from vyked.utils.stats import Stats, Aggregator


class Host:
    """Serves as a static entry point and provides the boilerplate required to host and run a Vyked Service.

    Example::

        Host.configure('SampleService')
        Host.attachService(SampleHTTPService())
        Host.run()

    """
    registry_host = None
    registry_port = None
    pubsub_host = None
    pubsub_port = None
    name = None
    ronin = False  # If true, the Vyked service runs solo without a registry

    _host_id = None
    _services = {'_tcp_service': None, '_http_service': None}
    _logger = logging.getLogger(__name__)

    @classmethod
    def configure(cls, name, registry_host: str="0.0.0.0", registry_port: int=4500,
                  pubsub_host: str="0.0.0.0", pubsub_port: int=6379):
        """ A convenience method for providing registry and pubsub(redis) endpoints

        :param name: Used for process name
        :param registry_host: IP Address for vyked-registry; default = 0.0.0.0
        :param registry_port: Port for vyked-registry; default = 4500
        :param pubsub_host: IP Address for pubsub component, usually redis; default = 0.0.0.0
        :param pubsub_port: Port for pubsub component; default= 6379
        :return: None
        """
        Host.name = name
        Host.registry_host = registry_host
        Host.registry_port = registry_port
        Host.pubsub_host = pubsub_host
        Host.pubsub_port = pubsub_port

    @classmethod
    @deprecated
    def attach_service(cls, service):
        """ Allows you to attach one TCP and one HTTP service

        deprecated:: 2.1.73 use http and tcp specific methods
        :param service: A vyked TCP or HTTP service that needs to be hosted
        """
        invalid_service = True
        _service_classes = {'_tcp_service': TCPService, '_http_service': HTTPService}
        for key, value in _service_classes.items():
            if isinstance(service, value):
                cls._services[key] = service
                invalid_service = False
                break
        if invalid_service:
            cls._logger.error('Invalid argument attached as service')
        cls._set_bus(service)

    @classmethod
    def attach_http_service(cls, http_service: HTTPService):
        """ Attaches a service for hosting
        :param http_service: A HTTPService instance
        """
        if cls._services['_http_service'] is None:
            cls._services['_http_service'] = http_service
            cls._set_bus(http_service)
        else:
            warnings.warn('HTTP service is already attached')

    @classmethod
    def attach_tcp_service(cls, tcp_service: TCPService):
        """ Attaches a service for hosting
        :param tcp_service: A TCPService instance
        """
        if cls._services['_tcp_service'] is None:
            cls._services['_tcp_service'] = tcp_service
            cls._set_bus(tcp_service)
        else:
            warnings.warn('TCP service is already attached')

    @classmethod
    def run(cls):
        """ Fires up the event loop and starts serving attached services
        """
        if not all(v is None for v in cls._services.values()):
            cls._set_host_id()
            cls._setup_logging()
            cls._set_process_name()
            cls._set_signal_handlers()
            cls._start_server()
        else:
            cls._logger.error('No services to host')

    @classmethod
    def _set_process_name(cls):
        from setproctitle import setproctitle
        setproctitle('vyked_{}_{}'.format(cls.name, cls._host_id))

    @classmethod
    def _stop(cls, signame: str):
        cls._logger.info('\ngot signal {} - exiting'.format(signame))
        asyncio.get_event_loop().stop()

    @classmethod
    def _set_signal_handlers(cls):
        asyncio.get_event_loop().add_signal_handler(getattr(signal, 'SIGINT'), partial(cls._stop, 'SIGINT'))
        asyncio.get_event_loop().add_signal_handler(getattr(signal, 'SIGTERM'), partial(cls._stop, 'SIGTERM'))

    @classmethod
    def _create_tcp_server(cls):
        if cls._services['_tcp_service']:
            ssl_context = cls._services['_tcp_service'].ssl_context
            host_ip, host_port = cls._services['_tcp_service'].socket_address
            task = asyncio.get_event_loop().create_server(partial(get_vyked_protocol,
                                                                  cls._services['_tcp_service'].tcp_bus),
                                                          host_ip, host_port, ssl=ssl_context)
            result = asyncio.get_event_loop().run_until_complete(task)
            return result

    @classmethod
    def _create_web_server(cls, service):
        if service:
            host_ip, host_port = service.socket_address
            ssl_context = service.ssl_context
            handler = cls._make_aiohttp_handler(service)
            task = asyncio.get_event_loop().create_server(handler, host_ip, host_port, ssl=ssl_context)
            return asyncio.get_event_loop().run_until_complete(task)

    @classmethod
    def _make_aiohttp_handler(cls, service):
        app = Application(loop=asyncio.get_event_loop())
        for each in service.__ordered__:
            # iterate all attributes in the service looking for http endpoints and add them
            fn = getattr(service, each)
            if callable(fn) and (getattr(fn, 'is_http_method', False) or getattr(fn, 'is_ws_method', False)):
                for path in fn.paths:
                    app.router.add_route(fn.method, path, fn)
                    if getattr(fn, 'is_http_method', False):
                            if service.cross_domain_allowed:
                                # add an 'options' for this specific path to make it CORS friendly
                                app.router.add_route('options', path, service.preflight_response)
        handler = app.make_handler(access_log=cls._logger)
        return handler

    @classmethod
    def _set_host_id(cls):
        from uuid import uuid4
        cls._host_id = uuid4()

    @classmethod
    def _start_server(cls):
        tcp_server = cls._create_tcp_server()
        http_server = cls._create_web_server(cls._services['_http_service'])
        server_dict = {'TCP': tcp_server, 'HTTP': http_server}
        if not cls.ronin:
            for service in cls._services.values():
                if service:
                    asyncio.get_event_loop().run_until_complete(service.tcp_bus.connect())
        for key, server in server_dict.items():
            if server:
                cls._logger.info('Serving ' + key + ' on {}'.format(server.sockets[0].getsockname()))
        cls._logger.info("Event loop running forever, press CTRL+c to interrupt.")
        cls._logger.info("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())
        try:
            asyncio.get_event_loop().run_forever()
        except Exception as e:
            print(e)
        finally:
            for server in server_dict.values():
                if server:
                    server.close()
                    asyncio.get_event_loop().run_until_complete(server.wait_closed())

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

    @classmethod
    def _set_bus(cls, service):
        registry_client = RegistryClient(asyncio.get_event_loop(), cls.registry_host, cls.registry_port)
        tcp_bus = TCPBus(registry_client)
        registry_client.conn_handler = tcp_bus
        pubsub_bus = PubSubBus(cls.pubsub_host, cls.pubsub_port, registry_client)
        registry_client.bus = tcp_bus
        _service_classes = {'tcp_host': TCPService, 'http_host': HTTPService}
        for key, value in _service_classes.items():
            if isinstance(service, value):
                tcp_bus.hosts[key] = service
        service.tcp_bus = tcp_bus
        service.pubsub_bus = pubsub_bus

    @classmethod
    def _setup_logging(cls):
        host = cls._services['_tcp_service'] if cls._services['_tcp_service'] else cls._services['_http_service']
        identifier = '{}_{}'.format(host.name, host.socket_address[1])
        setup_logging(identifier)
        Stats.service_name = host.name
        Stats.periodic_stats_logger()
        Aggregator.periodic_aggregated_stats_logger()
