from galaxy_py import glogging
from pslx.micro_service.rpc.client_base import ClientBase
from pslx.micro_service.container_backend.util import ContainerBackendUtil
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil


class ContainerBackendRPCClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = None

    def __init__(self, client_name, server_url):
        super().__init__(client_name=client_name, server_url=server_url)
        self._logger = glogging.get_logger(
            log_name=self.get_client_name(),
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'PSLX_INTERNAL/container_backend_client'
        )
        self._backend_folder = FileUtil.join_paths_to_dir(
            root_dir=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_METADATA_DIR'),
            base_name='PSLX_CONTAINER_BACKEND_TABLE'
        )

    def send_to_backend(self, snapshot):
        if FileUtil.is_local_path(self._backend_folder):
            ContainerBackendUtil.get_response_impl(
                backend_folder=self._backend_folder,
                request=snapshot
            )
        else:
            try:
                self.send_request(request=snapshot)
            except Exception as err:
                self._logger.error("Send to backend failed with error: " + str(err))
