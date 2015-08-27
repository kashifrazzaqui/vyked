import factory
import uuid
import pytest
from vyked.packet import ControlPacket


# class PacketFactory(factory.DictFactory):
#     pid = 0
#     type = ""
#     params = factory.Dict({})


# class ExampleFactor(factory.DictFactory):
#     a = factory.Sequence(lambda n: "service_%d" % n)


import logging
logging.getLogger("factory").setLevel(logging.WARN)

# class DependencyFactory(factory.ListFactory):
#     def __init__(self, params, *args, **kwargs):

#         super().__init__(args, kwargs)


class ServiceFactory(factory.DictFactory):

# class Meta:
#     exclude= ('_dependencies')

    service = factory.Sequence(lambda n: "service_%d" % n)
    version = "1.0.0"
    dependencies = factory.List([])
    host = factory.Sequence(lambda n: "192.168.0.%d" % n)
    port = factory.Sequence(lambda n: 4000 + n)
    node_id = factory.Sequence(lambda n: "node_%d" % n)
    type = 'tcp'

