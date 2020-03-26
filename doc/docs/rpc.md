RPC is a core component, and micro services built inside PSLX (for example [instant messaging](micro_services/instant_messaging.md), 
[email](micro_services/email.md), [rpc storage io](micro_services/rpc_storage_io.md)) are concrete implementations of RPC. To build
an RPC service using PSLX, one first needs to take a look at the abstract class implementation defined in [rpc_base](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/rpc/rpc_base.py).
The following functions are provided

```python
__init__(service_name, rpc_storage)
```
* Description: Create an RPC instance with given name and underlying storage.
* Arguments:
    1. service_name: the name of the rpc service. Needs to be unique.
    2. rpc_storage: the partitioner storage for storing the `GenericRPCRequestResponsePair`. If None, the `GenericRPCRequestResponsePair`
    won't be stored.
    
!!! info
    Function needs to be implemented by specific application.        
```python
get_response_and_status_impl(request)
```
* Description: get corresponding response and status from the request.
* Arguments:
    1. request: a proto message containing user defined request information.
* Explanation:
    1. The request and response can be any proto message type user defines.
    2. A concrete implementation of `RPCBase` needs to specify the type of request by setting `REQUEST_MESSAGE_TYPE` as a class
    level variable.
    
In addition to inheriting and implementing a RPC class, one also needs to create a client class inheriting [client_base](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/rpc/client_base.py).
The `client_base.py` provides the following functions

```python
__init__(client_name, server_url)
```
* Description: Create an client instance.
* Arguments:
    1. client_name: the name of the rpc client.
    2. server_url: the url to the rpc server.
    
```python
send_request(request, root_certificate)
```
* Description: Send request to the server and get response.
* Arguments:
    1. request: the request proto message.
    2. root_certificate: the root certificate for the SSL encryption. None if the rpc channel is insecure.

The user of PSLX can also define other function to create a specific `request` and call `send_request` inside the function. One example
can be found at [instant_messaging/client.py](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/instant_messaging/client.py).
In this example, a customized function `send_message` wraps the `send_request` and then allows user to pass more concrete arguments to
interact with rpc server.

Inside PSLX, RPC requests and responses will finally be parsed as `Any` proto message and hence the interface is universal. To create a
PSLX server, the user of PSLX needs to bind an RPC to a server using the methods provided by [generic_server](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/rpc/generic_server.py).

```python
_init__(server_name)
``` 
* Description: Create an generic server instance.
* Arguments:
    1. server_name: the name of the server. Needs to be unique.
    
```python
create_server(max_worker, server_url)
``` 
* Description: Create an generic server.
* Arguments:
    1. max_worker: the maximum number of workers for the server.
    2. server_url: the url to the rpc server.

!!! note
    Each generic server is only allowed to bind to one rpc instance.
 
```python
bind_rpc(rpc)
```
* Description: Bind an RPC instance to the server.
* Arguments:
    1. rpc: the rpc instance to be binded.
    
```python
start_server(private_key, certificate_chain)
```
* Description: Start running the server.
* Arguments:
    1. private_key: the private key of the SSL encryption.
    2. certificate_chain: the certificate chain of the SSL encryption.
