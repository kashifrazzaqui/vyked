from vyked.registry import Registry, Repository
import pytest
from .factories import ServiceFactory, EndpointFactory


@pytest.fixture
def registry():
    r = Registry(ip='192.168.1.1', port=4001, repository=Repository())
    return r


@pytest.fixture
def service(*args, **kwargs):
    return ServiceFactory(*args, **kwargs)


@pytest.fixture
def dependencies_from_services(*services):
    return [{'name': service['name'], 'version': service['version']} for service in services]


def endpoints_for_service(service, n):
    endpoints = []
    for _ in range(n):
        endpoint = EndpointFactory()
        endpoint['name'] = service['name']
        endpoint['version'] = service['version']
        endpoints.append(endpoint)
    return endpoints


@pytest.fixture
def service_a1():
    return ServiceFactory()


@pytest.fixture
def service_a2(service_a1):
    return ServiceFactory(service=service_a1['name'], version='1.0.1')


@pytest.fixture
def service_b1(service_a1):
    return ServiceFactory(dependencies=dependencies_from_services(service_a1))


@pytest.fixture
def service_b2(service_b1):
    return ServiceFactory(
        service=service_b1['name'], version='1.0.1', dependencies=dependencies_from_services(service_b1))


@pytest.fixture
def service_c1(service_a1, service_b1):
    return ServiceFactory(dependencies=dependencies_from_services(service_a1, service_b1))


@pytest.fixture
def service_c2(service_a2, service_b2, service_c1):
    return ServiceFactory(service=service_c1['name'], version='1.0.1',
                          dependencies=dependencies_from_services(service_a2, service_b2))


@pytest.fixture
def service_c3(service_a2, service_b2, service_c2):
    return ServiceFactory(service=service_c2['name'], version='1.0.1',
                          dependencies=dependencies_from_services(service_a2, service_b2))


@pytest.fixture
def service_d1(service_a1):
    return ServiceFactory(events=endpoints_for_service(service_a1, 1))
