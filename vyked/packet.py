from collections import defaultdict


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
                  'params': params}

        return packet

    @classmethod
    def send_instances(cls, service, version, instances):
        instances = [{'host': host, 'port': port, 'node': node, 'type': service_type} for host, port, node, service_type
                     in instances]
        instance_packet_params = {'service': service, 'version': version, 'instances': instances}
        return {'pid': cls._next_pid(), 'type': 'instances', 'params': instance_packet_params}

    @classmethod
    def deregister(cls, node_id, vendor):
        params = {'node_id': node_id, 'vendor': vendor}
        packet = {'pid': cls._next_pid(), 'type': 'deregister', 'params': params}
        return packet

    @classmethod
    def activated(cls, vendor_names, registered_services):
        vendors_packet = []
        for vendor_name in vendor_names:
            vendor_packet = defaultdict(list)
            for host, port, node, service_type in registered_services[vendor_name]:
                vendor_node_packet = {
                    'host': host,
                    'port': port,
                    'node_id': node,
                    'type': service_type
                }
                vendor_packet['name'] = vendor_name
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
    pass
