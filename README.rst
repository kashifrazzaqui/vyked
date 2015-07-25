Vyked
============================
Vyked is an asyncio based python framework for service oriented architectures.

Requirements
------------
- Python >= 3.3
- asyncio https://pypi.python.org/pypi/asyncio

Installation
------------

.. code-block:: bash

    $ pip install vyked
    
Vyked uses jsonstreamer_, a fast streaming JSON parser for Python that 
generates SAX-like events using fast C library yajl. To install yajl:

.. code-block:: bash

    $ git clone git@github.com:lloyd/yajl.git
    $ cd yajl
    $ ./configure && make install

.. _jsonstreamer: https://github.com/kashifrazzaqui/json-streamer

Examples
----------
Vyked allows you to host HTTP and TCP services. One of the core ideas of Service Oriented Architecture 
is a registry. A registry allows service providers to discover and communicate with consumers efficiently, creating a link between service providers and service customers.
To start the vyked registry:

.. code-block:: bash

    $ python -m vyked.registry

or :

.. code-block:: python
    
    from vyked import Registry
    
    registry = Registry('127.0.0.1', 4500)
    registry.start()

Sample TCP service:

.. code-block:: python

    from asyncio import sleep
    from vyked import Host, TCPService, api, publish
    
    class IdentityTCPService(TCPService):
        def __init__(self, ip, port):
            super(IdentityTCPService, self).__init__("IdentityService", 1, ip, port)
    
        @api
        def create(self, user_name, password):
            result = yield from sleep(5) #long running task
            if user_name is None:
                raise Exception('username cannot be none')
            return result
    
        @publish
        def password_changed(self, user_name):
            return locals()

Vyked uses aiohttp to setup HTTP server.

Sample HTTP service:

.. code-block:: python

    from aiohttp.web import Response, Request
    from vyked import Host, HTTPService, get, post
    
    class IdentityHTTPService(HTTPService):
        def __init__(self, ip, port):
            super(IdentityHTTPService, self).__init__("IdentityService", 1, ip, port)
    
        @get(path='/users/{username}')
        def get(self, request: Request):
            username = request.match_info.get('username')
            return Response(status=200, body=("Hello {}".format(username)).encode())
    
        @post(path='/users/{username}')
        def create(self, request: Request):
            data = yield from request.json()
            return Response(status=200, body=(json.dumps(data)).encode())
  
To start a service: 

.. code-block:: python

    if __name__ == '__main__':
        http = IdentityHTTPService('127.0.0.1', 4501)
        tcp = IdentityTCPService('127.0.0.1', 4502)
        Host.registry_host = '127.0.0.1'
        Host.registry_port = 4500
        Host.pubsub_host = '127.0.0.1'
        Host.pubsub_port = 6379
        Host.name = 'Identity'
        Host.attach_service(http)
        Host.attach_service(tcp)
        Host.run()

So far, the examples have only covered standalone services. But a service might interact with other services. 
To make such an interaction possible, vyked provides a TCP and HTTP client to interact with TCP and HTTP service respectively.


Sample TCP client for IdentityService we saw in the above example:

.. code-block:: python
    
    from vyked import Host, TCPService, TCPServiceClient, api, publish, request, subscribe
    import asyncio

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

Sample HTTP Client:

.. code-block:: python

    class Hello(HTTPServiceClient):
        def __init__(self):
            super(Hello, self).__init__('Hello', 1)
        
        @get()
        def person(self, name):
            path = '/{}'.format(name)
            params = {'key': 'value'}
            headers = {'Content-Type': 'application/json'}
            app_name = 'test'
            return locals()

License
-------
``vyked`` is offered under the MIT license.

Source code
-----------
The latest developer version is available in a github repository:
https://github.com/kashifrazzaqui/vyked
