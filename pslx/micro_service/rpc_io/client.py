from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import RPCIOResponse, RPCIORequest
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType
from pslx.util.proto_util import ProtoUtil


class RPCIOClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = RPCIOResponse
    STORAGE_TYPE = None

    WHITELISTED_KEY = [
        'fixed_size',
        'force_load',
        'num_line',
    ]

    def __init__(self, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)

    def read(self, file_or_dir_path, params=None, is_test=False):
        raise NotImplementedError


class DefaultStorageRPC(RPCIOClient):

    def read(self, file_or_dir_path, params=None, is_test=False):
        self.sys_log("Only support reading the whole file in RPC IO for DEFAULT_STORAGE type.")
        request = RPCIORequest()
        request.is_test = is_test
        request.type = StorageType.DEFAULT_STORAGE
        request.file_name = file_or_dir_path
        request.params['num_line'] = '-1'
        response = self.send_request(request=request)
        if response:
            return list(response.list_data.data)
        else:
            return []


class FixedSizeStorageRPC(RPCIOClient):

    def read(self, file_or_dir_path, params=None, is_test=False):
        request = RPCIORequest()
        request.is_test = is_test
        request.type = StorageType.FIXED_SIZE_STORAGE
        request.file_name = file_or_dir_path

        for key, val in params.items():
            if isinstance(val, str) or key in self.WHITELISTED_KEY:
                request.params[key] = str(val)

        response = self.send_request(request=request)
        if response:
            return list(response.list_data.data)
        else:
            return []


class ProtoTableStorageRPC(RPCIOClient):

    def read(self, file_or_dir_path, params=None, is_test=False):
        assert 'message_type' in params
        request = RPCIORequest()
        request.is_test = is_test
        request.type = StorageType.PROTO_TABLE_STORAGE
        request.file_name = file_or_dir_path
        self.RESPONSE_MESSAGE_TYPE = params['message_type']

        for key, val in params.items():
            if isinstance(val, str) or key in self.WHITELISTED_KEY:
                request.params[key] = str(val)

        request.params['message_type'] = ProtoUtil.infer_str_from_message_type(
            message_type=params['message_type']
        )

        response = self.send_request(request=request)
        return response


class PartitionerStorageRPC(RPCIOClient):

    def read(self, file_or_dir_path, params=None, is_test=False):
        assert 'PartitionerStorageType' in params

        request = RPCIORequest()
        request.is_test = is_test
        request.type = StorageType.PARTITIONER_STORAGE
        request.dir_name = file_or_dir_path

        for key, val in params.items():
            if isinstance(val, str) or key in self.WHITELISTED_KEY:
                request.params[key] = str(val)

        request.params['PartitionerStorageType'] = ProtoUtil.get_name_by_value(
            enum_type=PartitionerStorageType,
            value=params['PartitionerStorageType']
        )

        response = self.send_request(request=request)
        if response:
            return list(response.list_data.data)
        else:
            return []
