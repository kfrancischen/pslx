import os
from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import Status, StorageType, PartitionerStorageType
from pslx.schema.rpc_pb2 import RPCIORequest, RPCIOResponse
from pslx.storage.default_storage import DefaultStorage
from pslx.storage.fixed_size_storage import FixedSizeStorage
from pslx.storage.proto_table_storage import ProtoTableStorage
import pslx.storage.partitioner_storage as partitioner
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.proto_util import ProtoUtil


class RPCIO(RPCBase):
    REQUEST_MESSAGE_TYPE = RPCIORequest
    STORAGE_TYPE_TO_IMPL = {
        StorageType.DEFAULT_STORAGE: DefaultStorage,
        StorageType.FIXED_SIZE_STORAGE: FixedSizeStorage,
        StorageType.PROTO_TABLE_STORAGE: ProtoTableStorage,
        StorageType.PARTITIONER_STORAGE: {
            PartitionerStorageType.YEARLY: partitioner.YearlyPartitionerStorage,
            PartitionerStorageType.MONTHLY: partitioner.MonthlyPartitionerStorage,
            PartitionerStorageType.DAILY: partitioner.DailyPartitionerStorage,
            PartitionerStorageType.HOURLY: partitioner.HourlyPartitionerStorage,
            PartitionerStorageType.MINUTELY: partitioner.MinutelyPartitionerStorage,
        }
    }

    def __init__(self, request_storage):
        super().__init__(service_name=self.get_class_name(), request_storage=request_storage)
        self._lru_cache_tool = LRUCacheTool(
            max_capacity=os.getenv('PSLX_INTERNAL_CACHE', 100)
        )

    def send_request_impl(self, request):
        if request.is_test:
            return RPCIOResponse(), Status.SUCCEEDED

        read_params = request.params
        if request.type in [StorageType.DEFAULT_STORAGE, StorageType.PROTO_TABLE_STORAGE]:
            lru_key = (request.type, request.file_name)
            storage = self._lru_cache_tool.get(key=lru_key)
            if not storage:
                storage = self.STORAGE_TYPE_TO_IMPL[request.type]()
                if request.type == StorageType.PROTO_TABLE_STORAGE:
                    read_params['message_type'] = ProtoUtil.infer_message_type_from_str(
                        message_type_str=read_params['message_type']
                    )
                storage.initialize_from_file(file_name=request.file_name)
                self._lru_cache_tool.set(
                    key=lru_key,
                    value=storage
                )

        elif request.type == StorageType.FIXED_SIZE_STORAGE:
            lru_key = (request.type, request.file_name)
            if 'fixed_size' in read_params:
                lru_key += (read_params['fixed_size'], )

            storage = self._lru_cache_tool.get(key=lru_key)
            if not storage:
                if 'fixed_size' in read_params:
                    storage = self.STORAGE_TYPE_TO_IMPL[request.type](fixed_size=int(read_params['fixed_size']))
                else:
                    storage = self.STORAGE_TYPE_TO_IMPL[request.type]()
                storage.initialize_from_file(file_name=request.file_name)
                self._lru_cache_tool.set(
                    key=lru_key,
                    value=storage
                )

        else:
            lru_key = (read_params['PartitionerStorageType'], request.dir_name)
            storage = self._lru_cache_tool.get(key=lru_key)
            if not storage:
                partitioner_type = ProtoUtil.get_value_by_name_and_enum_name(
                    enum_type_str=PartitionerStorageType,
                    name=read_params['PartitionerStorageType']
                )
                storage = self.STORAGE_TYPE_TO_IMPL[request.type][partitioner_type]()
                storage.initialize_from_dir(dir_name=request.dir_name)
                self._lru_cache_tool.set(
                    key=lru_key,
                    value=storage
                )

            read_params.pop('PartitionerStorageType', None)

        data = storage.read(params=read_params)
        response = RPCIOResponse()
        for item in data:
            response.data.append(item)

        return response, Status.SUCCEEDED
