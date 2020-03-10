from pslx.micro_service.rpc_io.rpc import RPCIO
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.storage.partitioner_storage import MinutelyPartitionerStorage

if __name__ == "__main__":
    server_url = "localhost:11443"

    partitioner_dir = 'database/ttl=1/rpc_io_example'
    storage = MinutelyPartitionerStorage()
    storage.initialize_from_dir(dir_name=partitioner_dir)
    example_rpc = RPCIO(request_storage=storage)
    example_server = GenericServer(server_name='example')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
