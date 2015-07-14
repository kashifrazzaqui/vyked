class _Packet:
    _pid = 0

    @classmethod
    def _next_pid(cls):
        from uuid import uuid4
        return str(uuid4())

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
    def instances(cls, service, version):
        params = {'service': service, 'version': version}

        packet = {'pid': cls._next_pid(),
                  'type': 'get_instances',
                  'service': service,
                  'version': version,
                  'params': params}

        return packet

    @classmethod
    def pong(cls, node_id, count):

        packet = {'pid': cls._next_pid(), 'type': 'pong', 'node_id': node_id, 'count': count}

        return packet
