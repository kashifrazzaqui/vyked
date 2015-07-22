from collections import defaultdict
from uuid import uuid4

class _Packet:
    _pid = 0

    @classmethod
    def _next_pid(cls):
        from uuid import uuid4

        return str(uuid4())

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
    def registration(cls, ip: str, port: int, node_id, service: str, version: str, vendors, service_type: str):
        v = [{'service': vendor.name, 'version': vendor.version} for vendor in vendors]

        params = {'service': service,
                  'version': version,
                  'host': ip,
                  'port': port,
                  'node_id': node_id,
                  'vendors': v,
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
    def send_instances(cls, service, version, instances):
        instances = [{'host': host, 'port': port, 'node': node, 'type': service_type} for host, port, node, service_type
                     in instances]
        instance_packet_params = {'service': service, 'version': version, 'instances': instances}
        return {'pid': cls._next_pid(), 'type': 'instances', 'params': instance_packet_params}

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
