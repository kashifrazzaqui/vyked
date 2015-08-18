from unittest import mock


def test_register_independent_service(service1, registry):

    registry.register_service(packet={'params': service1}, registry_protocol=mock.Mock(),
                              host='192.168.1.1', port=2001)

    assert registry._repository.get_pending_services() == []


def test_register_dependent_service(service1, service2, registry):

    registry.register_service(packet={'params': service2}, registry_protocol=mock.Mock(),
                              host='192.168.1.1', port=2001)

    assert registry._repository.get_pending_services() != []

    registry.register_service(packet={'params': service1}, registry_protocol=mock.Mock(),
                              host='192.168.1.1', port=2001)
    assert registry._repository.get_pending_services() == []


def test_deregister_dependent_service(service1, service2, registry):

    registry.register_service(packet={'params': service2}, registry_protocol=mock.Mock(),
                              host='192.168.1.1', port=2001)

    registry.register_service(packet={'params': service1}, registry_protocol=mock.Mock(),
                              host='192.168.1.1', port=2001)

    assert registry._repository.get_pending_services() == []

    registry.deregister_service(service1['node_id'])
    assert registry._repository.get_pending_services() != []
