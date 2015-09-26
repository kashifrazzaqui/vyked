from vyked import Host, TCPService, TCPServiceClient, api, publish, request, subscribe
import asyncio

REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 4500

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

ACCOUNTS_HOST = '127.0.0.1'
ACCOUNTS_PORT = 4503


class AccountService(TCPService):
    def __init__(self, host, port):
        super(AccountService, self).__init__("AccountService", 1, host, port)

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
        super(IdentityClient, self).__init__("IdentityService", 1)

    @request
    def create(self, user_name, password):
        return locals()

    @subscribe
    def password_changed(self, user_name):
        print("Password changed event received")
        yield from asyncio.sleep(4)
        print("Password changed {}".format(user_name))

    def repeat_request(self):
        yield from asyncio.sleep(5)
        yield from self.create('test', 'test@123')
        yield from self.repeat_request()

if __name__ == '__main__':
    tcp = AccountService(ACCOUNTS_HOST, ACCOUNTS_PORT)
    tcp.clients = [IdentityClient()]
    Host.registry_host = REGISTRY_HOST
    Host.registry_port = REGISTRY_PORT
    Host.pubsub_host = REDIS_HOST
    Host.pubsub_port = REDIS_PORT
    Host.name = 'AccountService'
    Host.attach_service(tcp)
    Host.run()
