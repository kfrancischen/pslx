from galaxy_py import glogging
from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.micro_service.container_backend.util import ContainerBackendUtil
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import ContainerSnapshot
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil


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
        ContainerBackendUtil.get_response_impl(
            backend_folder=self._backend_folder,
            request=request,
            lru_cache=self._lru_cache_tool
        )
        return None, Status.SUCCEEDED
