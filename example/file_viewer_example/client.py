from pslx.micro_service.file_viewer.client import FileViewerRPCClient

if __name__ == "__main__":
    server_url = "localhost:11443"
    proto_viewer_client = FileViewerRPCClient(
        server_url=server_url
    )
    print(proto_viewer_client.view_file(
        file_path='database/snapshots/PROD/HelloWorldContainer__hello_world_container'
    ))
