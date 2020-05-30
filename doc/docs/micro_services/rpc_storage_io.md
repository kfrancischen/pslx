PSLX supports reading from any remote storage. The detailed RPC implementation is located at [rpc_io/rpc.py](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/rpc_io/rpc.py),
and all the storage types defined in [storage](../storage.md) are supported. One can also take a look at the example in
[example/rpc_io_example](https://github.com/kfrancischen/pslx/tree/master/example/rpc_io_example). Like other services, the
server can be launched with following code:

```python
from pslx.micro_service.rpc_io.rpc import RPCIO
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.storage.partitioner_storage import MinutelyPartitionerStorage
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil

if __name__ == "__main__":
    server_url = "localhost:11443"

    partitioner_dir = FileUtil.join_paths_to_dir_with_mode(
        root_dir=FileUtil.join_paths_to_dir(
                    root_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DATABASE'),
                    base_name='rpc_io'
                ),
        base_name='rpc_io_example',
        ttl=1
    )
    storage = MinutelyPartitionerStorage()
    storage.initialize_from_dir(dir_name=partitioner_dir)
    example_rpc = RPCIO(rpc_storage=storage)
    example_server = GenericServer(server_name='example')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
```

Note here, we also use a RPC storage (Proto Table) for `GenericRPCRequestResponsePair` storage.

The example clients are defined in [rpc_io_example/client.py](https://github.com/kfrancischen/pslx/blob/master/example/rpc_io_example/client.py)

```python
import datetime
from pslx.micro_service.rpc_io.client import DefaultStorageRPC, FixedSizeStorageRPC, ProtoTableStorageRPC,\
    PartitionerStorageRPC, ShardedProtoTableStorageRPC
from pslx.schema.enums_pb2 import PartitionerStorageType
from pslx.schema.snapshots_pb2 import NodeSnapshot

if __name__ == "__main__":

    server_url = "localhost:11443"

    file_name = "pslx/test/storage/test_data/test_default_storage_data.txt"
    example_client = DefaultStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=file_name
    ))

    example_client = FixedSizeStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=file_name,
        params={
            'fixed_size': 1,
            'num_line': 2,
            'force_load': True,
        }
    ))

    file_name = "pslx/test/storage/test_data/test_proto_table_data.pb"
    example_client = ProtoTableStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=file_name,
        params={
            'key': 'test',
            'message_type': NodeSnapshot,
            'proto_module': 'pslx.schema.snapshots_pb2',
        }
    ))

    file_name = "pslx/test/storage/test_data/test_proto_table_data.pb"
    example_client = ProtoTableStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=file_name,
        params={
            'key': 'test1',
            'message_type': NodeSnapshot,
            'proto_module': 'pslx.schema.snapshots_pb2',
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_1/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_1/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'read_oldest': True,
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_3/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read_range(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'start_time': datetime.datetime(2019, 1, 5),
            'end_time': datetime.datetime(2020, 1, 5),
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_4/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'is_proto_table': True,
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_4/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'is_proto_table': True,
            'read_oldest': True,
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_4/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read_range(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'start_time': datetime.datetime(2019, 1, 5),
            'end_time': datetime.datetime(2020, 1, 5),
            'is_proto_table': True
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_5/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'is_proto_table': True,
            'base_name': 'some_data.pb',
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_5/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'is_proto_table': True,
            'read_oldest': True,
            'base_name': 'some_data.pb',
        }
    ))

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_5/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read_range(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
            'start_time': datetime.datetime(2019, 1, 5),
            'end_time': datetime.datetime(2020, 1, 5),
            'is_proto_table': True
        }
    ))

    dir_name = "pslx/test/storage/test_data/sharded_proto_table_1"
    example_client = ShardedProtoTableStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'keys': ['test_0', 'test_1', 'test_100']
        }
    ))
```

We can see that in addition to the required parameter for the storage, we also need to pass the following parameters:
1. `PartitionerStorageType` for `PartitionerStorageRPC`.
2. `proto_module` for `ProtoTableStorageRPC` if one wants the message to be deserialized to the desired format.

As a side note, the `DefaultStorageRPC` will now ignore the parameter of `num_line` as in the RPC case, the function call will
always return the content of the whole underlying file.
