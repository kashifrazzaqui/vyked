from unittest import TestCase

from vyked import TCPService

class TestService(TCPService):
    def __init__(self, port):
        super().__init__('TestService', 1, host_port=port)

class ServiceTests(TestCase):

    def setUp(self):
        self._test_service = TestService(4500)

    def test_service_identifier(self):
        self.assertEquals(self._test_service.name, 'TestService'.lower())
        self.assertEquals(self._test_service.version, str(1))


