from services import ServiceHost, ServiceClient, api, publish, request, subscribe

class IdentityService(ServiceHost):
    @api
    def create(self, user_name, password, request_id=None, entity=None):
        """ This method is called by the bus when a client remotely calls this api """
        return True

    @publish
    def password_changed(self, user_name):
        """Calling this method causes the password_changed message to be sent on the bus"""
        return user_name


class IdentityClient(ServiceClient):
    @request
    def create(self, user_name, password):
        """Calling this method remotely invokes this api, must be empty and return locals"""
        return locals()


    @subscribe
    def password_changed(self, user_name):
        """This method is called by the bus when a ServiceHost publishes something"""
        pass

