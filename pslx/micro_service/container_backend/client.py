from galaxy_py import glogging
from pslx.micro_service.rpc.client_base import ClientBase
from pslx.util.env_util import EnvUtil


class ContainerBackendRPCClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = None

    def __init__(self, client_name, server_url):
        super().__init__(client_name=client_name, server_url=server_url)
        self._logger = glogging.get_logger(
            log_Name=self.get_client_name(),
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'INTERNAL/container_backend_client'
        )

    def send_to_backend(self, snapshot):
        try:
            self.send_request(request=snapshot)
        except Exception as err:
            self._logger.error("Send to backend failed with error: " + str(err))
