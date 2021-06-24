PSLX supports sending email over RPC. The implementation of the RPC is defined in [email/rpc.py](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/email/rpc.py),
and the following class methods are supported:

```python
add_email_credentials(credentials)
```
* Description: add new email credentials. The credentials can be composed using a function defined in [common_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/common_util.py).
* Arguments:
    1. credentials: the proto for email credential (see [schema](../schema.md))


There is also example about how to launch the RPC server in [example/email_example/server.py](https://github.com/kfrancischen/pslx/blob/master/example/email_example/server.py).

```python
import os

from pslx.micro_service.email.rpc import EmailRPC
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.util.common_util import CommonUtil

if __name__ == "__main__":
    server_url = "localhost:11443"
    example_rpc = EmailRPC(rpc_storage=None)
    credentials = CommonUtil.make_email_credentials(
        email_addr='alphahunter2019@gmail.com',
        password=os.getenv('PSLX_EMAIL_PWD', '')
    )
    example_rpc.add_email_credentials(credentials=credentials)
    example_server = GenericServer(server_name='example')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
```


The client of the email service also has an example int [example/email_example/client.py](https://github.com/kfrancischen/pslx/blob/master/example/email_example/client.py).
```python
from pslx.micro_service.email.client import EmailRPCClient


if __name__ == "__main__":
    server_url = "localhost:11443"
    email_client = EmailRPCClient(client_name='example email', server_url=server_url)
    email_client.send_email(
        from_email='alphahunter2019@gmail.com',
        to_email='kfrancischen@gmail.com',
        content='this is a test.'
    )
```
