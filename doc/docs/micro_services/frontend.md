PSLX supports a frontend UI for viewing proto and browsing containers. The implementation is based on Flask, and is in the
[folder](https://github.com/kfrancischen/pslx/tree/master/pslx/micro_service/frontend). The frontend UI is made of three parts:
1. RPC health checker: The index page will show the connection to the RPC servers, including the instant messaging server, email server and container backend RPC server.
2. Proto Viewer: view the content in the proto.
3. Container Backend: the status of the containers and operators.

On example way of starting a container backend is at [container_backend_example/server.py](https://github.com/kfrancischen/pslx/blob/master/example/container_backend_example/server.py):
```python
from pslx.micro_service.container_backend.rpc import ContainerBackendRPC
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.storage.partitioner_storage import MinutelyPartitionerStorage

if __name__ == "__main__":
    server_url = "localhost:11443"
    storage = MinutelyPartitionerStorage()
    storage.initialize_from_dir(dir_name="/galaxy/bb-d/pslx/test_data/storage")
    example_rpc = ContainerBackendRPC(rpc_storage=storage)
    example_server = GenericServer(server_name='example_backend')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
```

To start the frontend, one needs to create a YAML file containing the settings. An example of it can be found at
[frontend_example/frontend_config.yaml](https://github.com/kfrancischen/pslx/blob/master/example/frontend_example/frontend_config.yaml):
```yaml
CONTAINER_BACKEND_CONFIG:
  SERVER_URL: "localhost:11443"
  ROOT_CERTIFICATE_PATH: ""
USER_NAME: "guest"
PASSWORD: "guest"
GALAXY_VIEWER_URL: "http://192.168.1.112:8000"
```

The above `user_name` and `password` are the credentials used to login the frontend. In the second step, one needs to parse
the yaml file into a `FrontendConfig` proto. One can follow this script defined in [frontend_example/create_config.py](https://github.com/kfrancischen/pslx/blob/master/example/frontend_example/create_config.py):
```python
from pslx.util.common_util import CommonUtil
from pslx.util.file_util import FileUtil


def main():
    yaml_path = "example/frontend_example/frontend_config.yaml"
    proto_path = "example/frontend_example/frontend_config.pb"
    config = CommonUtil.make_frontend_config(yaml_path=yaml_path)
    print(config)
    FileUtil.write_proto_to_file(proto=config, file_name=proto_path)


if __name__ == "__main__":
    main()
```
Finally, one can launch the backend by pointing `PSLX_FRONTEND_CONFIG_PROTO_PATH` to the proto with
```python
from pslx.micro_service.frontend import pslx_frontend_ui_app

if __name__ == "__main__":
    pslx_frontend_ui_app.run(host='localhost', port=5001, debug=True)
```

The command is
```bash
PSLX_FRONTEND_CONFIG_PROTO_PATH=example/frontend_example/frontend_config.pb \
PYTHONPATH=. python example/frontend_example/run_frontend.py
```
