import datetime
from pslx.micro_service.rpc_io.client import DefaultStorageRPC, FixedSizeStorageRPC, ProtoTableStorageRPC,\
    PartitionerStorageRPC
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

    dir_name = "pslx/test/storage/test_data/yearly_partitioner_1/"
    example_client = PartitionerStorageRPC(client_name='example_rpc_io', server_url=server_url)
    print(example_client.read(
        file_or_dir_path=dir_name,
        params={
            'PartitionerStorageType': PartitionerStorageType.YEARLY,
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
