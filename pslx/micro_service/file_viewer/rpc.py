from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import FileViewerRPCRequest, FileViewerRPCResponse
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil


class FileViewerRPC(RPCBase):
    REQUEST_MESSAGE_TYPE = FileViewerRPCRequest

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)
        self._logger = LoggingTool(
            name=self.get_rpc_service_name(),
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def get_response_and_status_impl(self, request):
        file_path = request.file_path
        response = FileViewerRPCResponse()
        if FileUtil.is_file(path_name=file_path):
            file_name = FileUtil.die_if_file_not_exist(file_name=file_path)
            file_info = response.files_info.add()
            file_info.file_path = file_name
            file_info.file_size = FileUtil.get_file_size(file_name=file_name)
            file_info.modified_time = str(FileUtil.get_file_modified_time(file_name=file_name))
        else:
            dir_name = FileUtil.die_if_dir_not_exist(dir_name=file_path)
            sub_files = FileUtil.list_files_in_dir(dir_name=dir_name)
            for sub_file in sub_files:
                file_info = response.files_info.add()
                file_info.file_path = sub_file
                file_info.file_size = FileUtil.get_file_size(file_name=sub_file)
                file_info.modified_time = str(FileUtil.get_file_modified_time(file_name=sub_file))
            sub_dirs = FileUtil.list_dirs_in_dir(dir_name=dir_name)
            for sub_dir in sub_dirs:
                dirs_info = response.directories_info.add()
                dirs_info.file_path = sub_dir

        return response, Status.SUCCEEDED
