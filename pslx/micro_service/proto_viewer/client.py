from galaxy_py import glogging
from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import ProtoViewerRPCResponse, ProtoViewerRPCRequest
from pslx.util.env_util import EnvUtil


class ProtoViewerRPCClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = ProtoViewerRPCResponse

    def __init__(self, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)
        self._logger = glogging.get_logger(
            log_name=self.get_client_name(),
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'INTERNAL/proto_viewer_client'
        )

    def view_proto(self, proto_file_path, message_type, module):
        request = ProtoViewerRPCRequest()
        request.proto_file_path = proto_file_path
        request.message_type = message_type
        request.proto_module = module
        response = self.send_request(request=request)
        return {
            'proto_content': response.proto_content,
            'proto_file_path': response.file_info.file_path,
            'file_size': response.file_info.file_size,
            'modified_time': response.file_info.modified_time,
        }
