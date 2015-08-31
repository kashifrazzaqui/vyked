import factory
import logging
logging.getLogger("factory").setLevel(logging.WARN)


class ServiceFactory(factory.DictFactory):
    service = factory.Sequence(lambda n: "service_%d" % n)
    version = "1.0.0"
    dependencies = factory.List([])
    events = factory.List([])
    host = factory.Sequence(lambda n: "192.168.0.%d" % n)
    port = factory.Sequence(lambda n: 4000 + n)
    node_id = factory.Sequence(lambda n: "node_%d" % n)
    type = 'tcp'


class EndpointFactory(factory.DictFactory):
    endpoint = factory.Sequence(lambda n: "endpoint_%d" % n)
    strategy = factory.Iterator(['DESIGNATION', 'RANDOM'])
