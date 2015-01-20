from services import ServiceHost, ServiceClient, api, publish, request, listen

class IdentityService(ServiceHost):

    @api
    def create(self, user_name, password):
        return user_id



class IdentityClient(ServiceClient):

    @request
    def create(self, user_name, password):
        return locals()
