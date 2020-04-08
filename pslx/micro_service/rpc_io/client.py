from google.protobuf.any_pb2 import Any
from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import RPCIOResponse, RPCIORequest
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType
from pslx.tool.logging_tool import LoggingTool
from pslx.util.proto_util import ProtoUtil
from pslx.util.env_util import EnvUtil


class RPCIOClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = RPCIOResponse
    STORAGE_TYPE = None

    WHITELISTED_KEY = {
        'fixed_size',
        'force_load',
        'num_line',
        'start_time',
        'end_time',
        'is_proto_table',
    }

    def __init__(self, client_name, server_url):
        super().__init__(client_name=client_name, server_url=server_url)
        self._logger = LoggingTool(
            name=client_name,
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def get_storage_type(self):
        return self.STORAGE_TYPE

    def read(self, file_or_dir_path, params=None, is_test=False, root_certificate=None):
        raise NotImplementedError


class DefaultStorageRPC(RPCIOClient):
    STORAGE_TYPE = StorageType.DEFAULT_STORAGE

    def read(self, file_or_dir_path, params=None, is_test=False, root_certificate=None):
        self.sys_log("Only support reading the whole file in RPC IO for DEFAULT_STORAGE type.")
        request = RPCIORequest()
        request.is_test = is_test
        request.type = self.STORAGE_TYPE
        request.file_name = file_or_dir_path
        request.params['num_line'] = '-1'
        response = self.send_request(request=request, root_certificate=root_certificate)
        if response:
            return list(response.list_data.data)
        else:
            return []


class FixedSizeStorageRPC(RPCIOClient):
    STORAGE_TYPE = StorageType.FIXED_SIZE_STORAGE

    def read(self, file_or_dir_path, params=None, is_test=False, root_certificate=None):
        request = RPCIORequest()
        request.is_test = is_test
        request.type = self.STORAGE_TYPE
        request.file_name = file_or_dir_path

        for key, val in params.items():
            if isinstance(val, str) or key in self.WHITELISTED_KEY:
                request.params[key] = str(val)

        response = self.send_request(request=request, root_certificate=root_certificate)
        if response:
            return list(response.list_data.data)
        else:
            return []


class ProtoTableStorageRPC(RPCIOClient):
    STORAGE_TYPE = StorageType.PROTO_TABLE_STORAGE

    def read(self, file_or_dir_path, params=None, is_test=False, root_certificate=None):
        if 'message_type' in params:
            assert 'proto_module' in params
        request = RPCIORequest()
        request.is_test = is_test
        request.type = self.STORAGE_TYPE
        request.file_name = file_or_dir_path
        if 'message_type' in params:
            self.RESPONSE_MESSAGE_TYPE = params['message_type']
            request.params['message_type'] = ProtoUtil.infer_str_from_message_type(
                message_type=params['message_type']
            )
        else:
            self.RESPONSE_MESSAGE_TYPE = Any

        for key, val in params.items():
            if isinstance(val, str) or key in self.WHITELISTED_KEY:
                request.params[key] = str(val)

        response = self.send_request(request=request, root_certificate=root_certificate)
        return response


class PartitionerStorageRPC(RPCIOClient):
    STORAGE_TYPE = StorageType.PARTITIONER_STORAGE

    def read(self, file_or_dir_path, params=None, is_test=False, root_certificate=None):
        assert 'PartitionerStorageType' in params and 'start_time' not in params and 'end_time' not in params

        request = RPCIORequest()
        request.is_test = is_test
        request.type = self.STORAGE_TYPE
        request.dir_name = file_or_dir_path

        if 'is_proto_table' in params and params['is_proto_table']:
            params['is_proto_table'] = '1'
        else:
            params['is_proto_table'] = '0'

        if 'message_type' in params:
            request.params['message_type'] = ProtoUtil.infer_str_from_message_type(
                message_type=params['message_type']
            )

        for key, val in params.items():
            if isinstance(val, str) or key in self.WHITELISTED_KEY:
                request.params[key] = str(val)

        request.params['PartitionerStorageType'] = ProtoUtil.get_name_by_value(
            enum_type=PartitionerStorageType,
            value=params['PartitionerStorageType']
        )

        response = self.send_request(request=request, root_certificate=root_certificate)

        if response:
            if params['is_proto_table'] == '1':
                result = {}
                for key, val in dict(response.dict_data).items():
                    result[key] = val.data[0]
                return result
            else:
                return list(response.list_data.data)
        else:
            return None if params['is_proto_table'] == '1' else []

    def read_range(self, file_or_dir_path, params=None, is_test=False, root_certificate=None):
        assert 'PartitionerStorageType' in params and 'start_time' in params and 'end_time' in params
        if 'is_proto_table' in params and params['is_proto_table']:
            params['is_proto_table'] = '1'
        else:
            params['is_proto_table'] = '0'

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
        request.params['is_proto_table'] = params['is_proto_table']

        response = self.send_request(request=request, root_certificate=root_certificate)
        result = {}
        if response:
            for key, val in response.dict_data.items():
                if params['is_proto_table'] == '1':
                    result[key] = {}
                    for index in range(0, len(val.data) - 1, 2):
                        result[key][val.data[index]] = val.data[index + 1]
                else:
                    result[key] = list(val.data)

        return result
