from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import DataModelType, Status
from pslx.schema.snapshots_pb2 import ContainerSnapshot
from pslx.schema.storage_pb2 import ContainerBackendValue
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.storage.partitioner_storage import DailyPartitionerStorage
from pslx.tool.logging_tool import LoggingTool
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class ContainerBackendRPC(RPCBase):
    REQUEST_MESSAGE_TYPE = ContainerSnapshot

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)
        self._logger = LoggingTool(
            name='PSLX_CONTAINER_BACKEND_RPC',
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )
        self._lru_cache_tool = LRUCacheTool(
            max_capacity=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_CACHE')
        )
        self._backend_folder = FileUtil.join_paths_to_dir(
            root_dir=EnvUtil.get_pslx_env_variable('PSLX_DATABASE'),
            base_name='PSLX_CONTAINER_BACKEND_TABLE'
        )

    def get_response_and_status_impl(self, request):
        storage_value = ContainerBackendValue()
        storage_value.container_name = request.container_name
        storage_value.container_status = request.status
        for operator_name, operator_snapshot in dict(request.operator_snapshot_map).items():
            operator_info = ContainerBackendValue.OperatorInfo()
            operator_info.status = operator_snapshot.status
            for parent in operator_snapshot.node_snapshot.parents_names:
                operator_info.parents.append(parent)

            operator_info.start_time = operator_snapshot.start_time
            operator_info.end_time = operator_snapshot.end_time
            storage_value.operator_info_map[operator_name].CopyFrom(operator_info)

        storage_value.mode = request.mode
        storage_value.data_model = request.data_model
        storage_value.updated_time = str(TimezoneUtil.cur_time_in_pst())
        partitioner_dir = FileUtil.join_paths_to_dir_with_mode(
            root_dir=FileUtil.join_paths_to_dir(
                root_dir=self._backend_folder,
                base_name=ProtoUtil.get_name_by_value(
                    enum_type=DataModelType,
                    value=storage_value.data_model
                )
            ),
            base_name=storage_value.container_name,
            ttl=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_TTL')
        )
        storage = self._lru_cache_tool.get(key=partitioner_dir)
        if not storage:
            self.sys_log("Did not find the storage in cache. Making a new one...")
            storage = DailyPartitionerStorage()
            proto_table = ProtoTableStorage()
            storage.set_underlying_storage(storage=proto_table)
            self._lru_cache_tool.set(
                key=partitioner_dir,
                value=storage
            )
        else:
            self.sys_log("Found key in LRU cache.")
        storage.initialize_from_dir(dir_name=partitioner_dir)

        storage.write(
            data=[storage_value.container_name, storage_value],
            params={
                'overwrite': True,
                'make_partition': True,
            }
        )
        return None, Status.SUCCEEDED
