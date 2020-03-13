
from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import ProtoViewerRPCRequest, ProtoViewerRPCResponse
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


class ProtoViewerRPC(RPCBase):
    REQUEST_MESSAGE_TYPE = ProtoViewerRPCRequest

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)
        self._logger = LoggingTool(
            name=self.get_rpc_service_name(),
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def get_response_and_status_impl(self, request):
        proto_file = FileUtil.die_if_file_not_exist(file_name=request.proto_file_path)
        message_type = ProtoUtil.infer_message_type_from_str(
            message_type_str=request.message_type,
            modules=request.proto_module if request.proto_module else None
        )
        response = ProtoViewerRPCResponse()
        proto_message = FileUtil.read_proto_from_file(proto_type=message_type, file_name=proto_file)
        response.proto_content = ProtoUtil.message_to_text(proto_message=proto_message)
        response.proto_file_path = request.proto_file_path
        response.file_size = FileUtil.get_file_size(file_name=proto_file)
        response.modified_time = str(FileUtil.get_file_modified_time(file_name=proto_file))
        return response, Status.SUCCEEDED
