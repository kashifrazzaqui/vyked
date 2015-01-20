soa
===

Service Oriented Architecture Framework in Python


#### Packet Structure
    {
        'pid': <guid>, # service layer
        'entity': <app/service/user_id>, # routing key/partition for who this service is required, like 'user_id'
        'sender': <node address>, # service id, how a bus recognizes a process
        'type': <message|request|response|special>, # service layer
        'endpoint': <app/service/api_name>, #service layer
        'version': <api version>, #service layer
        'params': <json object>, #implementation
    }
        
####
 * pinging - half open socket checkt
 * heartbeats
 * routing
 
 
 #### Routing mechanism
 
 a routing address is described as "/app-name/service-name/entity-id" this is semantically describing that the packet
 should be routed to a service instance that is responsible for service a specific entity for a given app
