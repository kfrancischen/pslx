from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient

if __name__ == "__main__":
    server_url = "localhost:11443"
    proto_viewer_client = ProtoViewerRPCClient(
        server_url=server_url
    )
    proto_file_path = "database/rpc_io/PROD/ttl=1/rpc_io_example/2020/03/12/22/04/data.pb"
    message_type = 'ProtoTable'
    result = proto_viewer_client.view_proto(
        proto_file_path=proto_file_path,
        message_type=message_type,
        module=''
    )
    for key, val in result.items():
        print(key + ': ' + val)

    proto_file_path = "database/snapshots/PROD/ttl=7/CronBatchContainer__ttl_cleaner_container/" \
                      "SNAPSHOT_2020-03-12 11:30:00.018507-07:00_ttl_cleaner_container.pb"
    message_type = 'ContainerSnapshot'
    result = proto_viewer_client.view_proto(
        proto_file_path=proto_file_path,
        message_type=message_type,
        module='pslx.schema.snapshots_pb2'
    )
    for key, val in result.items():
        print(key + ': ' + val)
