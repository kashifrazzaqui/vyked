from unittest import mock
# from vyked.registry import Registry, Repository
# from vyked.packet import ControlPacket
import uuid
import cauldron
import psycopg2
import json

def json_file_to_dict(_file: str) -> dict:
    with open(_file) as config_file:
        config = json.load(config_file)
    return config

def service_registered_successfully(registry, *services):
    for service in services:
        service_entry = (
            service['host'], service['port'], service['node_id'], service['type'])
        try:
            entry1 = registry._repository.get_registered_services()
            entry = [(host, port, id, type) for (host, port, id, type, n, v) in entry1 if n == service['service']
                     and v == service['version']]
            # entry = registry._repository._registered_services[
            #     service['service']][service['version']]
            assert service_entry in entry
        except KeyError:
            raise
    return True


def no_pending_services(registry):
    return len(registry._repository.get_pending_services()) == 0


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

def test_setUp():
        # Delete and re-create database
    config = json_file_to_dict('./config.json')

    conn = psycopg2.connect(database=config['POSTGRES_DB'], user=config['POSTGRES_USER'],
                                     password=config['POSTGRES_PASS'], host=config['POSTGRES_HOST'],
                                     port=config['POSTGRES_PORT'])
    cur = conn.cursor()
    query = """
drop table services;
drop table subscriptions;
drop table dependencies;
drop table uptimes;


CREATE TABLE services(
   service_name VARCHAR(100) NOT NULL,
   version VARCHAR(100) NOT NULL,
   ip INET NULL, --can be made TEXT if INET does not work
   port INTEGER NULL,
   protocol VARCHAR(5) NULL,    --can be made ENUM for (TCP, HTTP, WS, ...)
   node_id VARCHAR(100) NULL,
   is_pending BOOLEAN DEFAULT TRUE,
   PRIMARY KEY (node_id)
);

CREATE TABLE subscriptions(
   subscriber_name VARCHAR(100) NOT NULL,
   subscriber_version VARCHAR(100) NOT NULL,
   subscribee_name VARCHAR(100) NOT NULL,
   subscribee_version VARCHAR(100) NOT NULL,
   event_name VARCHAR(100) NOT NULL,
   strategy VARCHAR(100) NOT NULL, -- can be made enum for (DESIGNATION, LEADER, RANDOM)
   PRIMARY KEY (subscriber_name, subscriber_version, subscribee_name, subscribee_version)
);

CREATE TABLE dependencies(
   child_name VARCHAR(100) NOT NULL,
   child_version VARCHAR(100) NOT NULL,
   parent_name VARCHAR(100) NOT NULL,
   parent_version VARCHAR(100) NOT NULL,
   PRIMARY KEY (child_name, child_version, parent_name, parent_version)
);

CREATE TABLE uptimes(
   node_id VARCHAR(100) NOT NULL,
   event_type VARCHAR(50) NOT NULL, -- can be made enum for (UPTIME, DOWNTIME)
   event_time INTEGER NOT NULL, -- change to timestamp if required
   PRIMARY KEY (node_id, event_type)
);
"""
    cur.execute(query)
    conn.commit()
    conn.close()
    return True

def test_register_independent_service(registry, service_a1):

    registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock())

    assert service_registered_successfully(registry, service_a1)
    assert no_pending_services(registry)


def test_register_dependent_service(registry, service_a1, service_b1):

    registry.register_service(
        packet={'params': service_b1}, registry_protocol=mock.Mock())
    assert not no_pending_services(registry)

    registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock())
    assert no_pending_services(registry)

    assert service_registered_successfully(registry, service_a1, service_b1)


def test_deregister_dependent_service(service_a1, service_b1, registry):
    registry.register_service(
        packet={'params': service_b1}, registry_protocol=mock.Mock())
    registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock())

    assert no_pending_services(registry)

    registry.deregister_service(service_a1['node_id'])
    assert not no_pending_services(registry)


def test_get_instances(service_a1, registry):
    registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock())

    protocol = mock.Mock()
    registry.get_service_instances(
        packet={'params': service_a1, 'request_id': str(uuid.uuid4())}, registry_protocol=protocol)

    assert instance_returned_successfully(
        protocol.send.call_args_list[0][0][0], service_a1)


def test_xsubscribe(service_a1, service_d1, registry):
    # assert service_d1 == {}
    registry.register_service(
        packet={'params': service_a1}, registry_protocol=mock.Mock())
    registry.register_service(
        packet={'params': service_d1}, registry_protocol=mock.Mock())
    registry._xsubscribe(packet={'params': service_d1})

    protocol = mock.Mock()
    params = {
        'service': service_a1['service'],
        'version': service_a1['version'],
        'endpoint': service_d1['events'][0]['endpoint']
    }
    registry.get_subscribers(packet={'params': params, 'request_id': str(uuid.uuid4())}, protocol=protocol)
    # assert protocol.send.call_args_list[0][0][0] == {}
    assert subscriber_returned_successfully(protocol.send.call_args_list[0][0][0], service_d1)
