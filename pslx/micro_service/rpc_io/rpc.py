import ast
from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import Status, StorageType, PartitionerStorageType
from pslx.schema.rpc_pb2 import RPCIORequest, RPCIOResponse
from pslx.storage.default_storage import DefaultStorage
from pslx.storage.fixed_size_storage import FixedSizeStorage
from pslx.storage.proto_table_storage import ProtoTableStorage
import pslx.storage.partitioner_storage as partitioner
from pslx.storage.sharded_proto_table_storage import ShardedProtoTableStorage
from pslx.tool.logging_tool import LoggingTool
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class RPCIO(RPCBase):
    REQUEST_MESSAGE_TYPE = RPCIORequest
    PARTITIONER_TYPE_TO_IMPL = {
        PartitionerStorageType.YEARLY: partitioner.YearlyPartitionerStorage,
        PartitionerStorageType.MONTHLY: partitioner.MonthlyPartitionerStorage,
        PartitionerStorageType.DAILY: partitioner.DailyPartitionerStorage,
        PartitionerStorageType.HOURLY: partitioner.HourlyPartitionerStorage,
        PartitionerStorageType.MINUTELY: partitioner.MinutelyPartitionerStorage,
    }

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)
        self._lru_cache_tool = LRUCacheTool(
            max_capacity=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_CACHE')
        )
        self._storage_type_to_impl_func = {
            StorageType.DEFAULT_STORAGE: self._default_storage_impl,
            StorageType.FIXED_SIZE_STORAGE: self._fixed_size_storage_impl,
            StorageType.PROTO_TABLE_STORAGE: self._proto_table_storage_impl,
            StorageType.PARTITIONER_STORAGE: self._partitioner_storage_impl,
            StorageType.SHARDED_PROTO_TABLE_STORAGE: self._sharded_proto_table_storage_impl,
        }
        self._logger = LoggingTool(
            name='PSLX_RPC_IO_RPC',
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def _default_storage_impl(self, request):
        self._logger.info("Getting request of default storage read.")
        read_params = dict(request.params)
        if 'num_line' in read_params:
            read_params['num_line'] = int(read_params['num_line'])

        lru_key = (request.type, request.file_name)
        storage = self._lru_cache_tool.get(key=lru_key)
        if not storage:
            self.sys_log("Did not find the storage in cache. Making a new one...")
            storage = DefaultStorage()
            storage.initialize_from_file(file_name=request.file_name)
            self._lru_cache_tool.set(
                key=lru_key,
                value=storage
            )
        else:
            self.sys_log("Found key in LRU cache.")
        self._logger.info('Current cache size ' + str(self._lru_cache_tool.get_cur_capacity()))

        response = RPCIOResponse()
        data = storage.read(params=read_params)
        rpc_list_data = RPCIOResponse.RPCListData()
        for item in data:
            rpc_data = rpc_list_data.data.add()
            rpc_data.string_data = item
        response.list_data.CopyFrom(rpc_list_data)
        return response

    def _fixed_size_storage_impl(self, request):
        self._logger.info("Getting request of fixed size storage read.")
        read_params = dict(request.params)
        if 'force_load' in read_params:
            read_params['force_load'] = ast.literal_eval(read_params['force_load'])
        if 'num_line' in read_params:
            read_params['num_line'] = int(read_params['num_line'])

        lru_key = (request.type, request.file_name)
        if 'fixed_size' in read_params:
            lru_key += (read_params['fixed_size'],)
        storage = self._lru_cache_tool.get(key=lru_key)
        if not storage:
            self.sys_log("Did not find the storage in cache. Making a new one...")
            if 'fixed_size' in read_params:
                storage = FixedSizeStorage(fixed_size=int(read_params['fixed_size']))
            else:
                storage = FixedSizeStorage()
            storage.initialize_from_file(file_name=request.file_name)
            self._lru_cache_tool.set(
                key=lru_key,
                value=storage
            )
        else:
            self.sys_log("Found key in LRU cache.")

        self._logger.info('Current cache size ' + str(self._lru_cache_tool.get_cur_capacity()))
        read_params.pop('fixed_size', None)
        response = RPCIOResponse()
        data = storage.read(params=read_params)
        rpc_list_data = RPCIOResponse.RPCListData()
        for item in data:
            rpc_data = rpc_list_data.data.add()
            rpc_data.string_data = item
        response.list_data.CopyFrom(rpc_list_data)
        return response

    def _proto_table_storage_impl(self, request):
        self._logger.info("Getting request of proto table storage read.")
        read_params = dict(request.params)
        if 'proto_module' in read_params:
            read_params['message_type'] = ProtoUtil.infer_message_type_from_str(
                message_type_str=read_params['message_type'],
                modules=read_params['proto_module']
            )
        else:
            read_params['message_type'] = ProtoUtil.infer_message_type_from_str(
                message_type_str=read_params['message_type']
            )

        lru_key = (request.type, request.file_name)
        storage = self._lru_cache_tool.get(key=lru_key)
        if not storage:
            self.sys_log("Did not find the storage in cache. Making a new one...")
            storage = ProtoTableStorage()
            storage.initialize_from_file(file_name=request.file_name)
            self._lru_cache_tool.set(
                key=lru_key,
                value=storage
            )
        else:
            self.sys_log("Found key in LRU cache.")
        self._logger.info('Current cache size ' + str(self._lru_cache_tool.get_cur_capacity()))
        read_params.pop('proto_module', None)
        return storage.read(params=read_params)

    def _sharded_proto_table_storage_impl(self, request):
        self._logger.info("Getting request of sharded proto table storage read.")
        keys = list(dict(request.params).keys())
        lru_key = (request.type, request.dir_name)
        storage = self._lru_cache_tool.get(key=lru_key)
        if not storage:
            self.sys_log("Did not find the storage in cache. Making a new one...")
            storage = ShardedProtoTableStorage()
            storage.initialize_from_dir(dir_name=request.dir_name)
            self._lru_cache_tool.set(
                key=lru_key,
                value=storage
            )
        else:
            self.sys_log("Found key in LRU cache.")
        self._logger.info('Current cache size ' + str(self._lru_cache_tool.get_cur_capacity()))
        data = storage.read(
            params={'keys': keys}
        )
        response = RPCIOResponse()
        for key, val in data.items():
            rpc_list_data = RPCIOResponse.RPCListData()
            rpc_data = rpc_list_data.data.add()
            rpc_data.proto_data.CopyFrom(val)
            response.dict_data[key].CopyFrom(rpc_list_data)
        return response

    def _partitioner_storage_impl(self, request):
        self._logger.info("Getting request of partitioner storage read.")
        read_params = dict(request.params)
        is_proto_table = True if read_params['is_proto_table'] == '1' else False
        if 'base_name' in read_params:
            base_name = read_params['base_name']
        else:
            base_name = 'data.pb' if is_proto_table else 'data'

        lru_key = (read_params['PartitionerStorageType'], request.dir_name)
        self._logger.info("Partitioner type is " + read_params['PartitionerStorageType'])
        storage = self._lru_cache_tool.get(key=lru_key)
        if not storage:
            self.sys_log("Did not find the storage in cache. Making a new one...")
            partitioner_type = ProtoUtil.get_value_by_name(
                enum_type=PartitionerStorageType,
                name=read_params['PartitionerStorageType']
            )
            storage = self.PARTITIONER_TYPE_TO_IMPL[partitioner_type]()
            storage.initialize_from_dir(dir_name=request.dir_name)
            self._lru_cache_tool.set(
                key=lru_key,
                value=storage
            )
        else:
            self.sys_log("Found key in LRU cache.")

        self._logger.info('Current cache size ' + str(self._lru_cache_tool.get_cur_capacity()))
        read_params.pop('PartitionerStorageType', None)
        read_params.pop('is_proto_table', None)

        if is_proto_table:
            proto_table_storage = ProtoTableStorage()
            storage.set_underlying_storage(storage=proto_table_storage)
        else:
            read_params['num_line'] = -1

        response = RPCIOResponse()
        if 'start_time' not in read_params:
            # calling read function
            if is_proto_table:
                # if underlying storage is proto table.
                if 'message_type' in read_params:
                    assert 'proto_module' in read_params
                    read_params['message_type'] = ProtoUtil.infer_message_type_from_str(
                        message_type_str=read_params['message_type'],
                        modules=read_params['proto_module']
                    )
                proto_storage = ProtoTableStorage()
                if 'read_oldest' in read_params:
                    proto_storage.initialize_from_file(
                        file_name=FileUtil.join_paths_to_file(
                            root_dir=storage.get_oldest_dir_in_root_directory(),
                            base_name=base_name
                        )
                    )
                else:
                    proto_storage.initialize_from_file(
                        file_name=FileUtil.join_paths_to_file(
                            root_dir=storage.get_latest_dir(),
                            base_name=base_name
                        )
                    )
                data = proto_storage.read_all()
                for key, val in data.items():
                    rpc_list_data = RPCIOResponse.RPCListData()
                    rpc_data = rpc_list_data.data.add()
                    rpc_data.proto_data.CopyFrom(val)
                    response.dict_data[key].CopyFrom(rpc_list_data)
            else:
                # if underlying storage is not proto table.
                default_storage = DefaultStorage()
                if 'read_oldest' in read_params:
                    default_storage.initialize_from_file(
                        file_name=FileUtil.join_paths_to_file(
                            root_dir=storage.get_oldest_dir_in_root_directory(),
                            base_name=base_name
                        )
                    )
                else:
                    default_storage.initialize_from_file(
                        file_name=FileUtil.join_paths_to_file(
                            root_dir=storage.get_latest_dir(),
                            base_name=base_name
                        )
                    )
                data = default_storage.read(params={
                    'num_line': -1,
                })
                rpc_list_data = RPCIOResponse.RPCListData()
                for item in data:
                    rpc_data = rpc_list_data.data.add()
                    rpc_data.string_data = item

                response.list_data.CopyFrom(rpc_list_data)
        else:
            # calling read_range function
            if 'start_time' in read_params:
                read_params['start_time'] = TimezoneUtil.cur_time_from_str(
                    time_str=read_params['start_time']
                )
            if 'end_time' in read_params:
                read_params['end_time'] = TimezoneUtil.cur_time_from_str(
                    time_str=read_params['end_time']
                )

            data = storage.read_range(params=read_params)
            if data:
                for key, val in data.items():
                    rpc_list_data = RPCIOResponse.RPCListData()
                    if is_proto_table:
                        for proto_key, any_message in val.items():
                            rpc_data = rpc_list_data.data.add()
                            rpc_data.string_data = proto_key

                            rpc_data = rpc_list_data.data.add()
                            rpc_data.proto_data.CopyFrom(any_message)
                    else:
                        for entry in val:
                            rpc_data = rpc_list_data.data.add()
                            rpc_data.string_data = entry

                    response.dict_data[key].CopyFrom(rpc_list_data)

        return response

    def get_response_and_status_impl(self, request):
        if request.is_test:
            return self.REQUEST_MESSAGE_TYPE(), Status.SUCCEEDED

        response = self._storage_type_to_impl_func[request.type](request=request)
        return response, Status.SUCCEEDED
