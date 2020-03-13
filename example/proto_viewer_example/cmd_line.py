"""
Example:
PYTHONPATH=. python example/proto_viewer_example/cmd_line.py \
--proto_file_path=database/rpc_io/PROD/ttl=1/rpc_io_example/2020/03/12/22/04/data.pb \
--message_type=ProtoTable \
--module=pslx.schema.storage_pb2
"""
import argparse
from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--proto_file_path', dest='proto_file_path', type=str, help='the path to the proto file.')
    parser.add_argument('--message_type', dest='message_type', type=str, help='the type of the message.')
    parser.add_argument('--module', dest='module', type=str, help='the module name')

    args = parser.parse_args()
    server_url = "localhost:11443"
    proto_viewer_client = ProtoViewerRPCClient(
        server_url=server_url
    )
    result = proto_viewer_client.view_proto(
        proto_file_path=args.proto_file_path,
        message_type=args.message_type,
        module=args.module
    )
    for key, val in result.items():
        print('\n' + key + ': ' + val)
