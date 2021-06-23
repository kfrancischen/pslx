from pslx.micro_service.proto_viewer.rpc import ProtoViewerRPC
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.storage.partitioner_storage import MinutelyPartitionerStorage

if __name__ == "__main__":
    server_url = "localhost:11444"
    storage = MinutelyPartitionerStorage()
    storage.initialize_from_dir(dir_name="/galaxy/bb-d/pslx/test_data/storage")
    example_rpc = ProtoViewerRPC(rpc_storage=storage)
    example_server = GenericServer(server_name='example')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
