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


class UserClient(TCPServiceClient):
    def __init__(self):
        super(UserClient, self).__init__("UserService", 1)

    @request
    def create(self, user_name, password):
        return locals()

    @subscribe
    def password_changed(self, user_name):
        print("Password changed event received")
        yield from asyncio.sleep(4)
        print("Password changed {}".format(user_name))


if __name__ == '__main__':
    tcp = AccountService(ACCOUNTS_HOST, ACCOUNTS_PORT)
    tcp.clients = [UserClient()]
    Host.configure('AccountService')
    Host.attach_service(tcp)
    Host.run()
