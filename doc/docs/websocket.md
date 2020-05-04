PSLX implements a basic Websocket client in [websocket_base.py](https://github.com/kfrancischen/pslx/blob/master/pslx/core/websocket_base.py),
where the following methods are supported:

```python
__init__(ws_url, params, logger)
```
* Description: Initialize a websocket client.
* Arguments:
    1. ws_url: the websocket URL.
    2. params: other parameters if necessary. It should be a dict.
    3. logger: the logger to record events.
    
```python
bind_to_op(op)
```
* Description: bind the websocket client to an operator.
* Arguments:
    1. op: the operator to be binded. In order to bind correctly, the operator needs to have a 
       `msg_parser(message)` function defined as one of the methods.

```python
start()
```
* Description: start to consume the websocket.