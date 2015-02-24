from asyncio import Future

from again.utils import unique_hex


# Service Client decorators
def subscribe(func):
    """
    use to listen for publications from a specific endpoint of a service
    """

    def wrapper(*args, **kwargs):
        pass  # TODO

    return wrapper


def request(func):
    """
    use to request an api call from a specific endpoint
    """

    def wrapper(*args, **kwargs):
        params = func(*args, **kwargs)
        self = params.pop('self')
        entity = params.pop('entity')
        request_id = unique_hex()
        params['request_id'] = request_id
        future = self._send_request(endpoint=func.__name__, entity=entity, params=params)
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
        self._publish(func.__name__, unique_hex(), entity, sender, params)
        return None

    return wrapper


def api(func):  # incoming
    """
    provide a request/response api
    receives any requests here and return value is the response
    all functions must have the following signature
        - request_id
        - entity (partition/routing key)
        followed by kwargs
    """

    def wrapper(*args, **kwargs):
        self = args[0]
        rid = kwargs.pop('request_id')
        entity = kwargs.pop('entity')
        from_id = kwargs.pop('from_id')
        result = None
        if len(kwargs):
            result = func(kwargs, request_id=rid, entity=entity)
        else:
            result = func(request_id=rid, entity=entity)
        return self._make_response_packet(request_id=rid, from_id=from_id, entity=entity, result=result)

    wrapper.is_api = True
    return wrapper


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

    @property
    def properties(self):
        return (self.app_name, self.name, self.version)

    def set_bus(self, bus):
        self._bus = bus


class ServiceClient(Service):
    _MSG_PKT_STR = 'message'
    _REQ_PKT_STR = 'request'

    def __init__(self, service_name, service_version, app_name):
        super(ServiceClient, self).__init__(service_name, service_version, app_name)
        self._pending_requests = {}

    def _send_message(self, endpoint, packet_id, entity, sender, params):
        packet = self._make_packet(ServiceClient._MSG_PKT_STR, endpoint, params, entity)
        self._bus.send(packet=packet)

    def _send_request(self, endpoint, entity, params):
        packet = self._make_packet(ServiceClient._REQ_PKT_STR, endpoint, params, entity)
        future = Future()
        request_id = params['request_id']
        self._pending_requests[request_id] = future
        self._bus.send(packet)
        return future

    def process_response(self, packet):
        params = packet['params']
        request_id = params['request_id']
        has_result = 'result' in params
        has_error = 'error' in params
        future = self._pending_requests.pop(request_id)
        if has_result:
            future.set_result(params['result'])
        elif has_error:
            exception = RequestException()
            exception.error = params['error']
            future.set_exception(exception)
        else:
            print('Invalid response to request:', packet)

    def _make_packet(self, packet_type, endpoint, params, entity):
        packet = {'pid': unique_hex(),
                  'app': self.app_name,
                  'service': self.name,
                  'entity': entity,
                  'endpoint': endpoint,
                  'version': self.version,
                  'type': packet_type,
                  'params': params}
        return packet


class ServiceHost(Service):
    def __init__(self, service_name, service_version, app_name):
        # TODO: to be multi-tenant make app_name a list
        super(ServiceHost, self).__init__(service_name, service_version, app_name)

    def is_for_me(self, packet:dict):
        app, service, version = packet['app'], packet['service'], packet['version']
        return app == self.app_name and \
               service == self.name and \
               version == self.version

    def _make_response_packet(self, request_id: str, from_id: str, entity:str, result:object):
        packet = {'pid': unique_hex(),
                  'to': from_id,
                  'entity': entity,
                  'type': 'response',
                  'params': {'request_id': request_id, 'result': result}}
        return packet


class RequestException(Exception):
    pass
