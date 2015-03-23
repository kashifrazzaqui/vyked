from vyked.bus import Bus
from vyked.services import TCPApplicationService, TCPServiceClient, api, publish, request, subscribe

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

IDENTITY_HOST = '127.0.0.1'
IDENTITY_PORT = 4501

ACCOUNTS_HOST = '127.0.0.1'
ACCOUNTS_PORT = 4502


class AccountService(TCPApplicationService):
    def __init__(self):
        super(AccountService, self).__init__("AccountService", "1", "Example")

    @api
    def authenticate(self, user_name, password):
        return user_name

    @publish
    def logged_out(self, user_name):
        return locals()


class AccountClient(TCPServiceClient):
    @request
    def authenticate(self, user_name, password):
        return locals()

    @subscribe
    def logged_out(self, user_name):
        pass


class IdentityClient(TCPServiceClient):
    def __init__(self):
        super(IdentityClient, self).__init__("IdentityService", "1", "Example")

    @request
    def create(self, user_name, password):
        return locals()

    @subscribe
    def password_changed(self, user_name):
        pass


def setup_accounts_service():
    bus = Bus(REGISTRY_HOST, REGISTRY_PORT)
    accounts_service = AccountService()
    identity_client = IdentityClient()
    bus.serve(accounts_service)
    bus.require([identity_client])
    bus.start(ACCOUNTS_HOST, ACCOUNTS_PORT)


if __name__ == '__main__':
    setup_accounts_service()

