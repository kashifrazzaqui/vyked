from vyked.registry import Registry, Repository
from unittest import mock


def test_register_independent_service():
    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'type1'}

    r = Registry(ip='192.168.1.1', port=4001, repository=Repository())

    r.register_service(packet={'params': s1}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    assert r._repository.get_pending_services() == []


def test_register_dependent_service():
    r = Registry(ip='192.168.1.1', port=4001, repository=Repository())

    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'tcp'}

    s2 = {'service': 'service2', 'version': '1.0.0',
          'vendors': [{'service': s1['service'], 'version': s1['version']}],
          'host': '192.168.1.3', 'port': 4003, 'node_id': 'n2', 'type': 'tcp'}

    r.register_service(packet={'params': s2}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    assert r._repository.get_pending_services() != []

    r.register_service(packet={'params': s1}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)
    assert r._repository.get_pending_services() == []


def test_deregister_dependent_service():
    r = Registry(ip='192.168.1.1', port=4001, repository=Repository())

    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'tcp'}

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


def test_versioning():
    r = Registry(ip='192.168.1.1', port=4001, repository=Repository())

    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'tcp'}

    s2 = {'service': 'service1', 'version': '1.0.2', 'vendors': [],
          'host': '192.168.1.2', 'port': 4003, 'node_id': 'n2', 'type': 'tcp'}

    s3 = {'service': 'service1', 'version': '2.8.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4004, 'node_id': 'n3', 'type': 'tcp'}

    r.register_service(packet={'params': s1}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    r.register_service(packet={'params': s2}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    r.register_service(packet={'params': s3}, registry_protocol=mock.Mock(),
                       host='192.168.1.1', port=2001)

    assert r._repository.get_versioned_instances('service1', '1.0.0') == [('192.168.1.1', 4002, 'n1', 'tcp')]
    assert r._repository.get_versioned_instances('service1', '1.1.0') == [('192.168.1.1', 4003, 'n2', 'tcp')]
    assert r._repository.get_versioned_instances('service1', '2.9.0') == [('192.168.1.1', 4004, 'n3', 'tcp')]


