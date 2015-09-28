from collections import defaultdict
from uuid import uuid4


class _Packet:
    _pid = 0

    @classmethod
    def _next_pid(cls):
        from uuid import uuid4

        return str(uuid4())

    @classmethod
    def ack(cls, request_id):
        return {'pid': cls._next_pid(), 'type': 'ack', 'request_id': request_id}

    @classmethod
    def pong(cls, node_id):
        return cls._get_ping_pong(node_id, 'pong')

    @classmethod
    def ping(cls, node_id):
        return cls._get_ping_pong(node_id, 'ping')

    @classmethod
    def _get_ping_pong(cls, node_id, packet_type):
        return {'pid': cls._next_pid(), 'type': packet_type, 'node_id': node_id}


class ControlPacket(_Packet):

    @classmethod
    def registration(cls, ip: str, port: int, node_id, service: str, version: str, dependencies, service_type: str):
        v = [{'service': vendor.name, 'version': vendor.version} for vendor in dependencies]

        params = {'service': service,
                  'version': version,
                  'host': ip,
                  'port': port,
                  'node_id': node_id,
                  'dependencies': v,
                  'type': service_type}

        packet = {'pid': cls._next_pid(), 'type': 'register', 'params': params}
        return packet

    @classmethod
    def get_instances(cls, service, version):
        params = {'service': service, 'version': version}
        packet = {'pid': cls._next_pid(),
                  'type': 'get_instances',
                  'service': service,
                  'version': version,
                  'params': params,
                  'request_id': str(uuid4())}

        return packet

    @classmethod
    def get_subscribers(cls, service, version, endpoint):
        params = {'service': service, 'version': version, 'endpoint': endpoint}
        packet = {'pid': cls._next_pid(),
                  'type': 'get_subscribers',
                  'params': params,
                  'request_id': str(uuid4())}
        return packet

    @classmethod
    def send_instances(cls, service, version, request_id, instances):
        instance_packet = [{'host': host, 'port': port, 'node': node, 'type': service_type} for
                           host, port, node, service_type in instances]
        instance_packet_params = {'service': service, 'version': version, 'instances': instance_packet}
        return {'pid': cls._next_pid(), 'type': 'instances', 'params': instance_packet_params, 'request_id': request_id}

    @classmethod
    # TODO : fix parsing on client side
    def deregister(cls, service, version, node_id):
        params = {'node_id': node_id, 'service': service, 'version': version}
        packet = {'pid': cls._next_pid(), 'type': 'deregister', 'params': params}
        return packet

    @classmethod
    def activated(cls, instances):
        vendors_packet = []
        for k, v in instances.items():
            vendor_packet = defaultdict(list)
            vendor_packet['name'] = k[0]
            vendor_packet['version'] = k[1]
            for host, port, node, service_type in v:
                vendor_node_packet = {
                    'host': host,
                    'port': port,
                    'node_id': node,
                    'type': service_type
                }
                vendor_packet['addresses'].append(vendor_node_packet)
            vendors_packet.append(vendor_packet)
        params = {
            'vendors': vendors_packet
        }
        packet = {'pid': cls._next_pid(),
                  'type': 'registered',
                  'params': params}
        return packet

    @classmethod
    def xsubscribe(cls, service, version, host, port, node_id, endpoints):
        params = {'service': service, 'version': version, 'host': host, 'port': port, 'node_id': node_id}
        events = [{'service': _service, 'version': _version, 'endpoint': endpoint, 'strategy': strategy} for
                  _service, _version, endpoint, strategy in endpoints]
        params['events'] = events
        packet = {'pid': cls._next_pid(),
                  'type': 'xsubscribe',
                  'params': params}
        return packet

    @classmethod
    def subscribers(cls, service, version, endpoint, request_id, subscribers):
        params = {'service': service, 'version': version, 'endpoint': endpoint}
        subscribers = [{'service': _service, 'version': _version, 'host': host, 'port': port, 'node_id': node_id,
                        'strategy': strategy} for _service, _version, host, port, node_id, strategy in subscribers]
        params['subscribers'] = subscribers
        packet = {'pid': cls._next_pid(),
                  'request_id': request_id,
                  'type': 'subscribers',
                  'params': params}
        return packet

    @classmethod
    def uptime(cls, uptimes):
        packet = {'pid': cls._next_pid(),
                  'type': 'uptime_report',
                  'params': dict(uptimes)}
        return packet

    @classmethod
    def new_instance(cls, service_name, version, host, port, node_id, type):
        params = {'service': service_name, 'version': version, 'host': host, 'port': port, 'node': node_id,
                  'type': type}
        return {'pid': cls._next_pid(),
                'type': 'new_instance',
                'params': params}


class MessagePacket(_Packet):

    @classmethod
    def request(cls, name, version, app_name, packet_type, endpoint, params, entity):
        return {'pid': cls._next_pid(),
                'app': app_name,
                'service': name,
                'version': version,
                'entity': entity,
                'endpoint': endpoint,
                'type': packet_type,
                'payload': params}

    @classmethod
    def publish(cls, publish_id, service, version, endpoint, payload):
        return {'pid': cls._next_pid(),
                'type': 'publish',
                'service': service,
                'version': version,
                'endpoint': endpoint,
                'payload': payload,
                'publish_id': publish_id}
