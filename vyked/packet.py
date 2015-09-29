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
    def pong(cls, node_id, payload=None):
        if payload:
            return cls._get_ping_pong(node_id, 'ping', payload=payload)
        return cls._get_ping_pong(node_id, 'pong')

    @classmethod
    def ping(cls, node_id, payload=None):
        if payload:
            return cls._get_ping_pong(node_id, 'ping', payload=payload)
        return cls._get_ping_pong(node_id, 'ping')

    @classmethod
    def _get_ping_pong(cls, node_id, packet_type, payload=None):
        if payload:
            return {'pid': cls._next_pid(), 'type': packet_type, 'node_id': node_id, 'payload': payload}
        return {'pid': cls._next_pid(), 'type': packet_type, 'node_id': node_id}


class ControlPacket(_Packet):

    @classmethod
    def registration(cls, ip: str, port: int, node_id, name: str, version: str, dependencies, service_type: str):
        v = [{'name': dependency.name, 'version': dependency.version} for dependency in dependencies]

        params = {'name': name,
                  'version': version,
                  'host': ip,
                  'port': port,
                  'node_id': node_id,
                  'dependencies': v,
                  'type': service_type}

        packet = {'pid': cls._next_pid(), 'type': 'register', 'params': params}
        return packet

    @classmethod
    def get_instances(cls, name, version):
        params = {'name': name, 'version': version}
        packet = {'pid': cls._next_pid(),
                  'type': 'get_instances',
                  'name': name,
                  'version': version,
                  'params': params,
                  'request_id': str(uuid4())}

        return packet

    @classmethod
    def get_subscribers(cls, name, version, endpoint):
        params = {'name': name, 'version': version, 'endpoint': endpoint}
        packet = {'pid': cls._next_pid(),
                  'type': 'get_subscribers',
                  'params': params,
                  'request_id': str(uuid4())}
        return packet

    @classmethod
    def send_instances(cls, name, version, request_id, instances):
        instance_packet = [{'host': host, 'port': port, 'node': node, 'type': service_type} for
                           host, port, node, service_type in instances]
        instance_packet_params = {'name': name, 'version': version, 'instances': instance_packet}
        return {'pid': cls._next_pid(), 'type': 'instances', 'params': instance_packet_params, 'request_id': request_id}

    @classmethod
    # TODO : fix parsing on client side
    def deregister(cls, name, version, node_id):
        params = {'node_id': node_id, 'name': name, 'version': version}
        packet = {'pid': cls._next_pid(), 'type': 'deregister', 'params': params}
        return packet

    @classmethod
    def activated(cls, instances):
        dependencies = []
        for k, v in instances.items():
            dependency = defaultdict(list)
            dependency['name'] = k[0]
            dependency['version'] = k[1]
            for host, port, node, service_type in v:
                dependency_node_packet = {
                    'host': host,
                    'port': port,
                    'node_id': node,
                    'type': service_type
                }
                dependency['addresses'].append(dependency_node_packet)
            dependencies.append(dependency)
        params = {
            'dependencies': dependencies
        }
        packet = {'pid': cls._next_pid(),
                  'type': 'registered',
                  'params': params}
        return packet

    @classmethod
    def xsubscribe(cls, name, version, host, port, node_id, endpoints):
        params = {'name': name, 'version': version, 'host': host, 'port': port, 'node_id': node_id}
        events = [{'name': _name, 'version': _version, 'endpoint': endpoint, 'strategy': strategy} for
                  _name, _version, endpoint, strategy in endpoints]
        params['events'] = events
        packet = {'pid': cls._next_pid(),
                  'type': 'xsubscribe',
                  'params': params}
        return packet

    @classmethod
    def subscribers(cls, name, version, endpoint, request_id, subscribers):
        params = {'name': name, 'version': version, 'endpoint': endpoint}
        subscribers = [{'name': _name, 'version': _version, 'host': host, 'port': port, 'node_id': node_id,
                        'strategy': strategy} for _name, _version, host, port, node_id, strategy in subscribers]
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
    def new_instance(cls, name, version, host, port, node_id, service_type):
        params = {'name': name, 'version': version, 'host': host, 'port': port, 'node_id': node_id,
                  'service_type': service_type}
        return {'pid': cls._next_pid(),
                'type': 'new_instance',
                'params': params}


class MessagePacket(_Packet):

    @classmethod
    def request(cls, name, version, app_name, packet_type, endpoint, params, entity):
        return {'pid': cls._next_pid(),
                'app': app_name,
                'name': name,
                'version': version,
                'entity': entity,
                'endpoint': endpoint,
                'type': packet_type,
                'payload': params}

    @classmethod
    def publish(cls, publish_id, name, version, endpoint, payload):
        return {'pid': cls._next_pid(),
                'type': 'publish',
                'name': name,
                'version': version,
                'endpoint': endpoint,
                'payload': payload,
                'publish_id': publish_id}
