Vyked
============================
.. image:: https://api.travis-ci.org/kashifrazzaqui/vyked.svg?branch=master
    :target: https://travis-ci.org/kashifrazzaqui/vyked
    

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
generates SAX-like events using the yajl C library. To install yajl:

.. code-block:: bash

    $ git clone git@github.com:lloyd/yajl.git
    $ cd yajl
    $ ./configure && make install

.. _jsonstreamer: https://github.com/kashifrazzaqui/json-streamer

Getting Started
---------------

Service Registry
^^^^^^^^^^^^^^^^
When Vyked services start they register with the service registry by advertising their name, ip,port and the names of other Vyked services they might require to function. The registry then co-ordinates all Vyked services and even balances loads across multiple instances of each service.

Vyked uses redis for pub-sub and registry storage, you must start a redis instance before you start the vyked registry.

You must start the Vyked registry before any services, as such:

.. code-block:: bash

    $ python -m vyked.registry

or :

.. code-block:: python
    
    from vyked import Registry
    
    registry = Registry('127.0.0.1', 4500)
    registry.start()

Service
^^^^^^^
Vyked allows you to host HTTP and TCP services. 

TCP Services
************
Vyked TCP services provide an RPC-like api by decorating typical class methods with decorators. There are two types of api's available,

* Request-Response using @api to provide such an api and @request to consume it from a remote client
* Publish-Subscribe using @publish to automatically publish an event and @subscribe to receive it at a remote client

The following basic examples illustrate these decorators for both Services and remote Clients.

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
            """ calling this method from within your code will cause a 'password_changed' event to be published
            to all subscribing services
            """
            # @publish decorated methods must return a dict of values to be published
            return locals() # easy way to return a dict containing all the params - in this case, user_name.


Sample HTTP service:

Vyked uses aiohttp to setup HTTP server.

.. code-block:: python

    from vyked import Host, HTTPService, get, post, Response, Request
    
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
        http = IdentityHTTPService('0.0.0.0', 4501)
        tcp = IdentityTCPService('0.0.0.0', 4502)
        Host.registry_host = '127.0.0.1'
        Host.registry_port = 4500
        Host.pubsub_host = '127.0.0.1'
        Host.pubsub_port = 6379
        Host.name = 'Identity'
        Host.attach_service(http)
        Host.attach_service(tcp)
        Host.run()

Client
^^^^^^^
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
            return locals() #@request requires a dict containing params describing the request payload
    
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

Documentation
-------------

View documentation at `read-the-docs`_

.. _read-the-docs: http://vyked.readthedocs.org/en/latest/

License
-------
``vyked`` is offered under the MIT license.

Source code
-----------
The latest developer version is available in a github repository:
https://github.com/kashifrazzaqui/vyked
