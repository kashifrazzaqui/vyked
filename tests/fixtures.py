from vyked.registry import Registry, Repository
import pytest


@pytest.fixture
def service1():
    s1 = {'service': 'service1', 'version': '1.0.0', 'vendors': [],
          'host': '192.168.1.2', 'port': 4002, 'node_id': 'n1', 'type': 'type1'}
    return s1


@pytest.fixture
def service2(service1):
    s2 = {'service': 'service2', 'version': '1.0.0',
          'vendors': [{'service': service1['service'], 'version': service1['version']}],
          'host': '192.168.1.3', 'port': 4003, 'node_id': 'n2', 'type': 'type1'}
    return s2


@pytest.fixture
def registry():
    r = Registry(ip='192.168.1.1', port=4001, repository=Repository())
    return r
