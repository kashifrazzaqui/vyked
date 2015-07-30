from vyked.registry import Registry
from unittest import mock


def test_register_independent_service():
    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'type1'}

    r = Registry(ip='192.168.1.1', port=4001)

    r.register_service(packet={'params': s1}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    assert r._repository.get_pending_services() == []


def test_register_dependent_service():
    r = Registry(ip='192.168.1.1', port=4001)

    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'type1'}

    s2 = {'service': 'service2', 'version': '1.0.0',
          'vendors': [{'service': s1['service'], 'version': s1['version']}],
          'host': '192.168.1.3', 'port': 4003, 'node_id': 'n2', 'type': 'type1'}

    r.register_service(packet={'params': s2}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    assert r._repository.get_pending_services() != []

    r.register_service(packet={'params': s1}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)
    assert r._repository.get_pending_services() == []


def test_deregister_dependent_service():
    r = Registry(ip='192.168.1.1', port=4001)

    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'type1'}

    s2 = {'service': 'service2', 'version': '1.0.0',
          'vendors': [{'service': s1['service'], 'version': s1['version']}],
          'host': '192.168.1.3', 'port': 4003, 'node_id': 'n2', 'type': 'type1'}

    r.register_service(packet={'params': s2}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    r.register_service(packet={'params': s1}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)
    assert r._repository.get_pending_services() == []

    r.deregister_service(s1['node_id'])
    assert r._repository.get_pending_services() != []
