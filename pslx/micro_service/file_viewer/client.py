from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import FileViewerRPCResponse, FileViewerRPCRequest


class FileViewerRPCClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = FileViewerRPCResponse

    def __init__(self, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)

    def view_file(self, file_path, root_certificate=None):
        request = FileViewerRPCRequest()
        request.file_path = file_path
        response = self.send_request(request=request, root_certificate=root_certificate)
        files_info, directories_info = {}, []
        for file_info in response.files_info:
            files_info[file_info.file_path] = {
                'file_path': file_info.file_path,
                'file_size': file_info.file_size,
                'modified_time': file_info.modified_time,
            }
        for directory_info in response.directories_info:
            directories_info.append(directory_info.file_path)
        return {
            'files_info': files_info,
            'directories_info': directories_info,
        }
