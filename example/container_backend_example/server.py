from pslx.micro_service.container_backend.rpc import ContainerBackendRPC
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.storage.partitioner_storage import MinutelyPartitionerStorage
from pslx.util.file_util import FileUtil

if __name__ == "__main__":
    server_url = "localhost:11443"
    partitioner_dir = FileUtil.join_paths_to_dir_with_mode(
        root_dir='database/container_backend/',
        base_name='container_backend_example',
        ttl=1
    )
    storage = MinutelyPartitionerStorage()
    storage.initialize_from_dir(dir_name=partitioner_dir)
    example_rpc = ContainerBackendRPC(rpc_storage=storage)
    example_server = GenericServer(server_name='example_backend')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
