from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import ProtoViewerRPCResponse, ProtoViewerRPCRequest


class ProtoViewerRPCClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = ProtoViewerRPCResponse

    def __init__(self, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)

    def view_proto(self, proto_file_path, message_type, module, root_certificate=None):
        request = ProtoViewerRPCRequest()
        request.proto_file_path = proto_file_path
        request.message_type = message_type
        request.proto_module = module
        response = self.send_request(request=request, root_certificate=root_certificate)
        return {
            'proto_content': response.proto_content,
            'proto_file_path': response.proto_file_path,
            'file_size': response.file_size,
            'modified_time': response.modified_time,
        }
