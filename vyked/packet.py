from uuid import uuid4

class _Packet:
    _pid = 0

    @classmethod
    def initialize(cls):
        import random
        cls._pid = random.randint(1000000, 9999999)

    @classmethod
    def _next_pid(cls):
        cls._pid += 1
        return cls._pid


class ControlPacket(_Packet):

    @classmethod
    def registration(cls, ip:str, port:int, node_id, service:str, version:str, vendors, service_type:str):
        v = []

        for vendor in vendors:
            v.append({'service': vendor.name, 'version': vendor.version})

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
                  'params': params,
                  'request_id': str(uuid4())}

        return packet
