from unittest import mock
# from vyked.registry import Registry, Repository
# from vyked.packet import ControlPacket
import uuid
import json
import asyncio

from collections import namedtuple

Record = namedtuple("Record", "ip port node_id protocol service_name version")
Record2 = namedtuple("Record", "service_name version")
Record3 = namedtuple("Record", "child_name child_version")
Record4 = namedtuple("Record", "subscriber_name subscriber_version ip port node_id strategy")
Record5 = namedtuple("Record", "ip port node_id protocol")

Service = namedtuple('Service', ['name', 'version', 'dependencies', 'host', 'port', 'node_id', 'type'])

@asyncio.coroutine
def asynchronizer(x):
    return x


def json_file_to_dict(_file: str) -> dict:
    with open(_file) as config_file:
        config = json.load(config_file)
    return config

def service_registered_successfully(registry, *services):
    for service in services:
        service_entry = (
            service['host'], service['port'], service['node_id'], service['type'])
        try:
            yield from asyncio.sleep(1)
            entry1 = yield from registry._repository.get_registered_services()
            # print("Get_registered_services")
            print(entry1)
            entry = [(host, port, id, type) for (host, port, id, type, n, v) in entry1 if n == service['service']
                     and v == service['version']]
            assert service_entry in entry
        except KeyError:
            raise
    return True

def no_pending_services(registry):
    yield from asyncio.sleep(1)
    res = yield from registry._repository.get_pending_services()
    return (len(res) == 0)


def instance_returned_successfully(response, service):
    instance = (
        service['host'], service['port'], service['node_id'], service['type'])
    for returned_instance in response['params']['instances']:
        t = (
            returned_instance['host'], returned_instance['port'], returned_instance['node'], returned_instance['type'])
        if instance == t:
            return True

    return False


def subscriber_returned_successfully(response, service):
    service_t = (service['host'], service['port'], service['node_id'], service['service'], service['version'])
    for s in response['params']['subscribers']:
        subscriber_t = (s['host'], s['port'], s['node_id'], s['service'], s['version'])

        if service_t == subscriber_t:
            return True
    return False

@asyncio.coroutine
def test_register_independent_service(registry, service_a1):

    pr = mock.MagicMock()

    get_pending_instances_response =[['node_0']]
    remove_pending_instance_response =[None]
    get_consumers_response =[set()]
    get_vendors_response =[[], []]
    register_service_response =[None]
    get_pending_services_response =[[Record2(service_name='service_0', version='1.0.0')], []]
    get_registered_services_response =[[Record(ip='192.168.0.0', port=4000, node_id='node_0', protocol='tcp', service_name='service_0', version='1.0.0')]]

    get_pending_instances_async_response = [asynchronizer(x) for x in get_pending_instances_response]
    remove_pending_instance_async_response = [asynchronizer(x) for x in remove_pending_instance_response]
    get_consumers_async_response = [asynchronizer(x) for x in get_consumers_response]
    get_vendors_async_response = [asynchronizer(x) for x in get_vendors_response]
    register_service_async_response = [asynchronizer(x) for x in register_service_response]
    get_pending_services_async_response = [asynchronizer(x) for x in get_pending_services_response]
    get_registered_services_async_response = [asynchronizer(x) for x in get_registered_services_response]

    pr.get_pending_instances.side_effect =get_pending_instances_async_response
    pr.remove_pending_instance.side_effect =remove_pending_instance_async_response
    pr.get_consumers.side_effect =get_consumers_async_response
    pr.get_vendors.side_effect =get_vendors_async_response
    pr.register_service.side_effect =register_service_async_response
    pr.get_pending_services.side_effect =get_pending_services_async_response
    pr.get_registered_services.side_effect =get_registered_services_async_response

    registry._repository = pr

    asyncio.async(registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock()))

    assert (yield from service_registered_successfully(registry, service_a1))

    assert (yield from no_pending_services(registry))




@asyncio.coroutine
def test_register_dependent_service(registry, service_a1, service_b1):

    pr = mock.MagicMock()

    get_pending_instances_response =[['node_2'], ['node_1'], ['node_2']]
    get_versioned_instances_response =[[], [Record5(ip='192.168.0.1', port=4001, node_id='node_1', protocol='tcp')], [Record5(ip='192.168.0.1', port=4001, node_id='node_1', protocol='tcp')]]
    get_consumers_response =[set(), {Record3(child_name='service_2', child_version='1.0.0')}]
    is_pending_response =[True]
    get_vendors_response =[[{'version': '1.0.0', 'service': 'service_1'}], [], [], [{'version': '1.0.0', 'service': 'service_1'}], [{'version': '1.0.0', 'service': 'service_1'}]]
    register_service_response =[None, None]
    get_pending_services_response =[[Record2(service_name='service_2', version='1.0.0')], [Record2(service_name='service_2', version='1.0.0')], [Record2(service_name='service_1', version='1.0.0'), Record2(service_name='service_2', version='1.0.0')], []]
    get_registered_services_response =[[Record(ip='192.168.0.0', port=4000, node_id='node_0', protocol='tcp', service_name='service_0', version='1.0.0'), Record(ip='192.168.0.1', port=4001, node_id='node_1', protocol='tcp', service_name='service_1', version='1.0.0'), Record(ip='192.168.0.2', port=4002, node_id='node_2', protocol='tcp', service_name='service_2', version='1.0.0')], [Record(ip='192.168.0.0', port=4000, node_id='node_0', protocol='tcp', service_name='service_0', version='1.0.0'), Record(ip='192.168.0.1', port=4001, node_id='node_1', protocol='tcp', service_name='service_1', version='1.0.0'), Record(ip='192.168.0.2', port=4002, node_id='node_2', protocol='tcp', service_name='service_2', version='1.0.0')]]
    remove_pending_instance_response =[None, None]

    get_pending_instances_async_response = [asynchronizer(x) for x in get_pending_instances_response]
    get_versioned_instances_async_response = [asynchronizer(x) for x in get_versioned_instances_response]
    get_consumers_async_response = [asynchronizer(x) for x in get_consumers_response]
    is_pending_async_response = [asynchronizer(x) for x in is_pending_response]
    get_vendors_async_response = [asynchronizer(x) for x in get_vendors_response]
    register_service_async_response = [asynchronizer(x) for x in register_service_response]
    get_pending_services_async_response = [asynchronizer(x) for x in get_pending_services_response]
    get_registered_services_async_response = [asynchronizer(x) for x in get_registered_services_response]
    remove_pending_instance_async_response = [asynchronizer(x) for x in remove_pending_instance_response]

    pr.get_pending_instances.side_effect =get_pending_instances_async_response
    pr.get_versioned_instances.side_effect =get_versioned_instances_async_response
    pr.get_consumers.side_effect =get_consumers_async_response
    pr.is_pending.side_effect =is_pending_async_response
    pr.get_vendors.side_effect =get_vendors_async_response
    pr.register_service.side_effect =register_service_async_response
    pr.get_pending_services.side_effect =get_pending_services_async_response
    pr.get_registered_services.side_effect =get_registered_services_async_response
    pr.remove_pending_instance.side_effect =remove_pending_instance_async_response

    registry._repository = pr

    asyncio.async(registry.register_service(
        packet={'params': service_b1}, registry_protocol=mock.Mock()))

    assert not (yield from no_pending_services(registry))

    asyncio.async(registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock()))

    assert (yield from no_pending_services(registry))

    assert (yield from service_registered_successfully(registry, service_a1, service_b1))




@asyncio.coroutine
def test_deregister_dependent_service(service_a1, service_b1, registry):

    pr = mock.MagicMock()

    get_pending_instances_response =[['node_3'], ['node_3'], ['node_4'], ['node_4']]
    remove_pending_instance_response =[None, None, None, None]
    get_consumers_response =[{Record3(child_name='service_4', child_version='1.0.0')}, set(), {Record3(child_name='service_4', child_version='1.0.0')}, {Record3(child_name='service_4', child_version='1.0.0')}]
    is_pending_response =[True]
    get_node_response =[Service(name='service_3', version='1.0.0', dependencies=[], host='192.168.0.3', port=4003, node_id='node_3', type='tcp')]
    get_vendors_response =[[], [], [], [{'version': '1.0.0', 'service': 'service_3'}], [], [{'version': '1.0.0', 'service': 'service_3'}], [{'version': '1.0.0', 'service': 'service_3'}], [{'version': '1.0.0', 'service': 'service_3'}]]
    register_service_response =[None, None]
    get_pending_services_response =[[Record2(service_name='service_3', version='1.0.0'), Record2(service_name='service_4', version='1.0.0')], [Record2(service_name='service_3', version='1.0.0'), Record2(service_name='service_4', version='1.0.0')], [], [Record2(service_name='service_4', version='1.0.0')]]
    get_versioned_instances_response =[[Record5(ip='192.168.0.3', port=4003, node_id='node_3', protocol='tcp')], [Record5(ip='192.168.0.3', port=4003, node_id='node_3', protocol='tcp')], [Record5(ip='192.168.0.3', port=4003, node_id='node_3', protocol='tcp')], [Record5(ip='192.168.0.3', port=4003, node_id='node_3', protocol='tcp')]]
    get_instances_response =[[], [Record5(ip='192.168.0.4', port=4004, node_id='node_4', protocol='tcp')], [Record5(ip='192.168.0.4', port=4004, node_id='node_4', protocol='tcp')]]
    remove_node_response =[None]

    get_pending_instances_async_response = [asynchronizer(x) for x in get_pending_instances_response]
    remove_pending_instance_async_response = [asynchronizer(x) for x in remove_pending_instance_response]
    get_consumers_async_response = [asynchronizer(x) for x in get_consumers_response]
    is_pending_async_response = [asynchronizer(x) for x in is_pending_response]
    get_node_async_response = [asynchronizer(x) for x in get_node_response]
    get_vendors_async_response = [asynchronizer(x) for x in get_vendors_response]
    register_service_async_response = [asynchronizer(x) for x in register_service_response]
    get_pending_services_async_response = [asynchronizer(x) for x in get_pending_services_response]
    get_versioned_instances_async_response = [asynchronizer(x) for x in get_versioned_instances_response]
    get_instances_async_response = [asynchronizer(x) for x in get_instances_response]
    remove_node_async_response = [asynchronizer(x) for x in remove_node_response]

    pr.get_pending_instances.side_effect =get_pending_instances_async_response
    pr.remove_pending_instance.side_effect =remove_pending_instance_async_response
    pr.get_consumers.side_effect =get_consumers_async_response
    pr.is_pending.side_effect =is_pending_async_response
    pr.get_node.side_effect =get_node_async_response
    pr.get_vendors.side_effect =get_vendors_async_response
    pr.register_service.side_effect =register_service_async_response
    pr.get_pending_services.side_effect =get_pending_services_async_response
    pr.get_versioned_instances.side_effect =get_versioned_instances_async_response
    pr.get_instances.side_effect =get_instances_async_response
    pr.remove_node.side_effect =remove_node_async_response

    registry._repository = pr

    asyncio.async(registry.register_service(
        packet={'params': service_b1}, registry_protocol=mock.Mock()))
    asyncio.async(registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock()))

    assert (yield from no_pending_services(registry))

    asyncio.async(registry.deregister_service(service_a1['node_id']))

    assert not (yield from no_pending_services(registry))




@asyncio.coroutine
def test_get_instances(service_a1, registry):

    pr = mock.MagicMock()

    get_pending_instances_response =[['node_4'], ['node_5']]
    get_versioned_instances_response =[[]]
    get_consumers_response =[set()]
    remove_pending_instance_response =[None]
    get_vendors_response =[[{'version': '1.0.0', 'service': 'service_3'}], [], []]
    register_service_response =[None]
    get_pending_services_response =[[Record2(service_name='service_4', version='1.0.0'), Record2(service_name='service_5', version='1.0.0')]]
    get_instances_response =[[Record5(ip='192.168.0.5', port=4005, node_id='node_5', protocol='tcp')]]

    get_pending_instances_async_response = [asynchronizer(x) for x in get_pending_instances_response]
    get_versioned_instances_async_response = [asynchronizer(x) for x in get_versioned_instances_response]
    get_consumers_async_response = [asynchronizer(x) for x in get_consumers_response]
    remove_pending_instance_async_response = [asynchronizer(x) for x in remove_pending_instance_response]
    get_vendors_async_response = [asynchronizer(x) for x in get_vendors_response]
    register_service_async_response = [asynchronizer(x) for x in register_service_response]
    get_pending_services_async_response = [asynchronizer(x) for x in get_pending_services_response]
    get_instances_async_response = [asynchronizer(x) for x in get_instances_response]

    pr.get_pending_instances.side_effect =get_pending_instances_async_response
    pr.get_versioned_instances.side_effect =get_versioned_instances_async_response
    pr.get_consumers.side_effect =get_consumers_async_response
    pr.remove_pending_instance.side_effect =remove_pending_instance_async_response
    pr.get_vendors.side_effect =get_vendors_async_response
    pr.register_service.side_effect =register_service_async_response
    pr.get_pending_services.side_effect =get_pending_services_async_response
    pr.get_instances.side_effect =get_instances_async_response


    registry._repository = pr


    asyncio.async(registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock()))
    yield from asyncio.sleep(1)
    protocol = mock.Mock()
    asyncio.async(registry.get_service_instances(
        packet={'params': service_a1, 'request_id': str(uuid.uuid4())}, registry_protocol=protocol))
    yield from asyncio.sleep(1)

    assert instance_returned_successfully(
        protocol.send.call_args_list[0][0][0], service_a1)

@asyncio.coroutine
def test_xsubscribe(service_a1, service_d1, registry):
    # assert service_d1 == {}

    pr = mock.MagicMock()

    get_pending_instances_response =[['node_4'], ['node_4'], ['node_6'], ['node_6'], ['node_8'], ['node_8']]
    get_versioned_instances_response =[[], []]
    get_consumers_response =[set(), set()]
    remove_pending_instance_response =[None, None, None, None]
    xsubscribe_response =[None]
    get_vendors_response =[[{'version': '1.0.0', 'service': 'service_3'}], [{'version': '1.0.0', 'service': 'service_3'}], [], [], [], [], [], [], [], []]
    register_service_response =[None, None]
    get_pending_services_response =[[Record2(service_name='service_4', version='1.0.0'), Record2(service_name='service_6', version='1.0.0'), Record2(service_name='service_8', version='1.0.0')], [Record2(service_name='service_4', version='1.0.0'), Record2(service_name='service_6', version='1.0.0'), Record2(service_name='service_8', version='1.0.0')]]
    get_subscribers_response =[[Record4(subscriber_name='service_8', subscriber_version='1.0.0', ip='192.168.0.8', port=4008, node_id='node_8', strategy='DESIGNATION')]]

    get_pending_instances_async_response = [asynchronizer(x) for x in get_pending_instances_response]
    get_versioned_instances_async_response = [asynchronizer(x) for x in get_versioned_instances_response]
    get_consumers_async_response = [asynchronizer(x) for x in get_consumers_response]
    remove_pending_instance_async_response = [asynchronizer(x) for x in remove_pending_instance_response]
    xsubscribe_async_response = [asynchronizer(x) for x in xsubscribe_response]
    get_vendors_async_response = [asynchronizer(x) for x in get_vendors_response]
    register_service_async_response = [asynchronizer(x) for x in register_service_response]
    get_pending_services_async_response = [asynchronizer(x) for x in get_pending_services_response]
    get_subscribers_async_response = [asynchronizer(x) for x in get_subscribers_response]

    pr.get_pending_instances.side_effect =get_pending_instances_async_response
    pr.get_versioned_instances.side_effect =get_versioned_instances_async_response
    pr.get_consumers.side_effect =get_consumers_async_response
    pr.remove_pending_instance.side_effect =remove_pending_instance_async_response
    pr.xsubscribe.side_effect =xsubscribe_async_response
    pr.get_vendors.side_effect =get_vendors_async_response
    pr.register_service.side_effect =register_service_async_response
    pr.get_pending_services.side_effect =get_pending_services_async_response
    pr.get_subscribers.side_effect =get_subscribers_async_response


    registry._repository = pr


    asyncio.async(registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock()))
    asyncio.async(registry.register_service(
        packet={'params': service_d1}, registry_protocol=mock.Mock()))
    asyncio.async(registry._xsubscribe(packet={'params': service_d1}))
    yield from asyncio.sleep(1)

    protocol = mock.Mock()
    params = {
        'service': service_a1['service'],
        'version': service_a1['version'],
        'endpoint': service_d1['events'][0]['endpoint']
    }
    asyncio.async(registry.get_subscribers(packet={'params': params, 'request_id': str(uuid.uuid4())}, protocol=protocol))
    yield from asyncio.sleep(1)

    # assert protocol.send.call_args_list[0][0][0] == {}
    assert subscriber_returned_successfully(protocol.send.call_args_list[0][0][0], service_d1)

    
