from pslx.micro_service.rpc.client_base import ClientBase
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil


class ContainerBackendClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = None

    def __init__(self, client_name, server_url, root_certificate=None):
        super().__init__(client_name=client_name, server_url=server_url)
        self._root_certificate = root_certificate
        self._logger = LoggingTool(
            name=self.get_class_name(),
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def send_to_backend(self, snapshot):
        try:
            self.send_request(request=snapshot, root_certificate=self._root_certificate)
        except Exception as err:
            self._logger.error("Send to backend failed with error: " + str(err))
