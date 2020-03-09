from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import RPCIOResponse, RPCIORequest
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType
from pslx.util.proto_util import ProtoUtil


class RPCIOClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = RPCIOResponse

    def __init__(self, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)

    def read_remote_storage(self, storage_type, file_or_dir_path, params, is_test=False):
        request = RPCIORequest()
        request.is_test = is_test
        request.type = storage_type
        for key, val in params:
            if not isinstance(val, str):
                request.params[key] = val

        if request.type == StorageType.PARTITIONER_STORAGE:
            assert 'PartitionerStorageType' in params
            request.dir_name = file_or_dir_path
            request.params['PartitionerStorageType'] = ProtoUtil.get_name_by_value(
                enum_type=PartitionerStorageType,
                value=params['PartitionerStorageType']
            )
        else:
            request.file_name = file_or_dir_path

        if request.type == StorageType.PROTO_TABLE_STORAGE:
            assert 'message_tye' in params
            request.params['message_type'] = str(params['message_type'])
        if request.type == StorageType.DEFAULT_STORAGE:
            if 'num_line' in params:
                self.sys_log("Only support reading the whole file in RPC IO.")
                request.params['num_line'] = '-1'

        self.send_request(request=request)
