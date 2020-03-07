from pslx.micro_service.instant_messaging.rpc import InstantMessagingRPC
from pslx.micro_service.rpc.generic_server import GenericServer


if __name__ == "__main__":
    server_url = "localhost:11443"

    example_rpc = InstantMessagingRPC(request_storage=None)
    example_server = GenericServer(server_name='example')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
