from galaxy_py import glogging
from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import ContainerSnapshot
from pslx.schema.storage_pb2 import ContainerBackendValue
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.timezone_util import TimezoneUtil


class ContainerBackendRPC(RPCBase):
    REQUEST_MESSAGE_TYPE = ContainerSnapshot

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)
        self._logger = glogging.get_logger(
            log_name='PSLX_CONTAINER_BACKEND_RPC',
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'PSLX_INTERNAL/container_backend_rpc'
        )
        self._lru_cache_tool = LRUCacheTool(
            max_capacity=int(EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_CACHE'))
        )
        self._backend_folder = FileUtil.join_paths_to_dir(
            root_dir=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_METADATA_DIR'),
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
            operator_info.log_file = operator_snapshot.log_file
            storage_value.operator_info_map[operator_name].CopyFrom(operator_info)

        storage_value.mode = request.mode
        storage_value.data_model = request.data_model
        storage_value.updated_time = str(TimezoneUtil.cur_time_in_pst())
        storage_value.start_time = request.start_time
        storage_value.end_time = request.end_time
        storage_value.log_file = request.log_file
        storage_value.run_cell = request.run_cell
        for key in request.counters:
            storage_value.counters[key] = request.counters[key]
        storage_value.ttl = int(EnvUtil.get_pslx_env_variable('PSLX_BACKEND_CONTAINER_TTL'))

        storage = self._lru_cache_tool.get(key=storage_value.container_name)
        if not storage:
            self._SYS_LOGGER.info("Did not find the storage in cache. Making a new one...")
            storage = ProtoTableStorage()
            storage.initialize_from_file(
                file_name=FileUtil.join_paths_to_file(
                    root_dir=self._backend_folder,
                    base_name=storage_value.container_name + '.pb'
                )
            )
            self._lru_cache_tool.set(
                key=self._backend_folder,
                value=storage
            )
        else:
            self._SYS_LOGGER.info("Found key in LRU cache.")

        storage.write(
            data={storage_value.container_name: storage_value}
        )
        return None, Status.SUCCEEDED
