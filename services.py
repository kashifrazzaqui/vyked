from asyncio import Future

from again.utils import unique_hex


# Service Client decorators
def subscribe(func):
    """
    use to listen for publications from a specific endpoint of a service,
    this method receives a publication from a remote service
    """

    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper.is_subscribe = True
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

    wrapper.is_request = True
    return wrapper


# Service Host Decorators

def publish(func):
    """
    publish the return value of this function as a message from this endpoint
    """

    def wrapper(*args, **kwargs):  # outgoing
        payload = func(*args, **kwargs)
        self = payload.pop('self')
        self._publish(func.__name__, payload)
        return None

    wrapper.is_publish = True

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
    _PUB_PKT_STR = 'publish'
    _REQ_PKT_STR = 'request'
    _RES_PKT_STR = 'response'

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


class TCPServiceClient(Service):
    def __init__(self, service_name, service_version, app_name):
        super(TCPServiceClient, self).__init__(service_name, service_version, app_name)
        self._pending_requests = {}

    def _send_request(self, endpoint, entity, params):
        packet = self._make_request_packet(Service._REQ_PKT_STR, endpoint, params, entity)
        future = Future()
        request_id = params['request_id']
        self._pending_requests[request_id] = future
        self._bus.send(packet)
        return future

    def process_packet(self, packet):
        if packet['type'] == Service._RES_PKT_STR:
            self._process_response(packet)
        elif packet['type'] == Service._PUB_PKT_STR:
            self._process_publication(packet)
        else:
            print('Invalid packet', packet)


    def _process_response(self, packet):
        payload = packet['payload']
        request_id = payload['request_id']
        has_result = 'result' in payload
        has_error = 'error' in payload
        future = self._pending_requests.pop(request_id)
        if has_result:
            future.set_result(payload['result'])
        elif has_error:
            exception = RequestException()
            exception.error = payload['error']
            future.set_exception(exception)
        else:
            print('Invalid response to request:', packet)


    def _process_publication(self, packet):
        endpoint = packet['endpoint']
        func = getattr(self, endpoint)
        func(**packet['payload'])

    def _make_request_packet(self, packet_type, endpoint, params, entity):
        packet = {'pid': unique_hex(),
                  'app': self.app_name,
                  'service': self.name,
                  'version': self.version,
                  'entity': entity,
                  'endpoint': endpoint,
                  'type': packet_type,
                  'payload': params}
        return packet


class TCPServiceHost(Service):
    def __init__(self, service_name, service_version, app_name):
        # TODO: to be multi-tenant make app_name a list
        super(TCPServiceHost, self).__init__(service_name, service_version, app_name)

    def is_for_me(self, packet:dict):
        app, service, version = packet['app'], packet['service'], packet['version']
        return app == self.app_name and \
               service == self.name and \
               version == self.version

    def _publish(self, publication_name, payload):
        packet = self._make_publish_packet(Service._PUB_PKT_STR, publication_name, payload)
        self._bus.send(packet)

    def _make_response_packet(self, request_id: str, from_id: str, entity:str, result:object):
        packet = {'pid': unique_hex(),
                  'to': from_id,
                  'entity': entity,
                  'type': Service._RES_PKT_STR,
                  'payload': {'request_id': request_id, 'result': result}}
        return packet

    def _make_publish_packet(self, packet_type:str, publication_name:str, payload:dict):
        packet = {'pid': unique_hex(),
                  'app': self.app_name,
                  'service': self.name,
                  'version': self.version,
                  'endpoint': publication_name,
                  'type': packet_type,
                  'payload': payload}
        return packet


class RequestException(Exception):
    pass
