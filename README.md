KOZO - japanese for structure
===

Service Oriented Architecture Framework in Python

#### Service Registry
##### Service activation protocol
    Two phase process - register and activate
* ServiceHost setups its servers
* ServiceHost requests registration with the ServiceRegistry by providing its node_id, full_service_name
and a list of all its dependencies
* Registry responds to the registration request of service host when it has atleast one instance for each dependency
 registered but not necessarily activated
* ServiceHost receives registration response and connects with dependencies
* ServiceHost then sends a request to activate and waits
* Registry responds to activation request if all dependencies have requested activation 

#### Packet Structure
    {
        'to': <node_address> # bus layer
        'from': <node_address>, # bus layer
        
        'pid': <guid>, # added at the service layer
        'app': <app_name>,
        'service':<service_name>,
        'entity': <user_id>, # user_id is routing key/partition for who this service is required
        'endpoint': <api_name>, #added at the service layer
        'version': <api version>, #added at the service layer
        'type': <message|request|response|special>, # added at the service layer
        'params': <json object>, #added at the specific ServiceHost/Client implementation
    }
        
####
 * pinging - half open socket checkt
 * heartbeats
 * routing
 
 
 #### Routing mechanism
 
 a routing address aka 'service address' is described as "/app-name/service-name/entity-id" this is semantically describing that the packet
 should be routed to a service instance that is responsible for service a specific entity for a given app
