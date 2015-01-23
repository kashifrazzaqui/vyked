import uuid

from asyncio import Future


# Service Client decorators
def listen(func):  # incoming
    """
    use to listen for publications from a specific endpoint of a service
    """

    def wrapper(*args, **kwargs):
        pass  # TODO

    return wrapper


def request(func):  # outgoing
    """
    use to request an api call from a specific endpoint
    """

    def wrapper(*args, **kwargs):
        params = func(*args, **kwargs)
        self = params.pop('self')
        entity = params.pop('entity', '*')  # broadcast to any available service instance
        future = self._request_dispatch(endpoint=func.__name__, entity=entity, params=params)
        return future

    return wrapper


# Service Host Decorators

def publish(func):
    """
    publish a message from this endpoint
    """

    def wrapper(*args, **kwargs):  # outgoing
        params = func(*args, **kwargs)
        self = params.pop('self')
        entity = params.pop('entity')
        sender = params.pop('sender')
        self._publish(func.__name__, uuid.uuid4(), entity, sender, params)
        return None

    return wrapper


def api(func):  # incoming
    """
    provide a request/response api
    receives any requests here and return value is the response
    all functions must have the following signature
        - request_id
        - sender (service id)
        - entity (partition/routing key)
        followed by kwargs
    """

    def wrapper(*args, **kwargs):
        self = args[0]
        rid = kwargs.pop('request_id')
        sender = kwargs.pop('sender')
        result = func(kwargs, request_id=rid, sender=sender)
        packet = {'response_id': rid, 'entity': sender, 'params': {'result': result}}
        self._response(packet)

    return wrapper()


def _make_address(app_name, service_name, node_id):
    return '/{}/{}/{}'.format(app_name, service_name, node_id)


def _parse_address(address):
    app, service, node_id = address.split('/')
    return app, service, node_id


class Service:
    def __init__(self, service_name, service_version, app_name):
        self._service_name = service_name
        self._service_version = service_version
        self._app_name = app_name

    @property
    def name(self):
        return self._service_name

    @property
    def version(self):
        return self._service_version

    @property
    def app_name(self):
        return self._app_name

    def set_bus(self, bus):
        self._bus = bus


class ServiceClient(Service):
    _MSG_PKT_STR = 'message'
    _REQ_PKT_STR = 'request'

    def __init__(self, service_name, service_version, app_name):
        super(ServiceClient, self).__init__(service_name, service_version, app_name)
        self._pending_requests = {}

    def _message_dispatch(self, endpoint, packet_id, entity, sender, params):
        packet = self._make_packet(ServiceClient._MSG_PKT_STR, endpoint, params, entity)
        self._bus.send(packet=packet)

    def _request_dispatch(self, endpoint, entity, params):
        packet = self._make_packet(ServiceClient._REQ_PKT_STR, endpoint, params, entity)
        future = Future()
        request_id = params['request_id']
        self._pending_requests[request_id] = future
        self._bus.send(packet=packet)
        return future

    def _make_packet(self, packet_type, endpoint, params, entity):
        to_address = _make_address(self.app_name, self.name, entity)
        packet = {'pid': uuid.uuid4(), 'type': packet_type, 'entity': to_address, 'endpoint': endpoint,
                  'version': self.version, 'params': params}
        return packet


class ServiceHost(Service):
    def __init__(self, service_name, service_version, app_name):
        super(ServiceHost, self).__init__(service_name, service_version, app_name)

    def _dev_null(self, packet):
        print("Unknown endpoint: can't route packet".format(packet))

    def allocate(self, packet):
        app, service, node_id = _parse_address(packet['to'])
        if app == self.app_name and service == self.name and packet['version'] == self.version:
            func = getattr(self, packet['endpoint'], self._dev_null)
            func(packet['pid'], node_id, packet['sender'], packet['params'])
        else:
            print("Service constraints violated - can't route packet: {}".format(packet))

