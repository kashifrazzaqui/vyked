from services import ServiceHost, ServiceClient, api, publish, request, subscribe

class IdentityService(ServiceHost):
    @api
    def create(self, user_name, password, request_id=None, entity=None):
        return True

    @publish
    def password_changed(self, user_name):
        pass


class IdentityClient(ServiceClient):
    @request
    def create(self, user_name, password):
        return locals()


    @subscribe
    def password_changed(self, user_name):
        pass