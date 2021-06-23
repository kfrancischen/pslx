from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient

if __name__ == "__main__":
    server_url = "localhost:11444"
    proto_viewer_client = ProtoViewerRPCClient(
        server_url=server_url
    )
    proto_file_path = "/galaxy/bb-d/pslx/test_data/test_proto_table_data_2.pb"
    message_type = 'ProtoTable'
    result = proto_viewer_client.view_proto(
        proto_file_path=proto_file_path,
        message_type=message_type,
        module=''
    )
    for key, val in result.items():
        print(key + ': ' + str(val))

    proto_file_path = "/galaxy/bb-d/ttl=7d/snapshots/SNAPSHOT_2021-06-22 14:10:58.343147-07:00_hello_world_container.pb"
    message_type = 'ContainerSnapshot'
    result = proto_viewer_client.view_proto(
        proto_file_path=proto_file_path,
        message_type=message_type,
        module='pslx.schema.snapshots_pb2'
    )
    for key, val in result.items():
        print(key + ': ' + str(val))
