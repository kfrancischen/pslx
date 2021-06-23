from galaxy_py import glogging
from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import EmailPRCRequest
from pslx.util.env_util import EnvUtil


class EmailRPCClient(ClientBase):

    def __init__(self, client_name, server_url):
        super().__init__(client_name=client_name, server_url=server_url)
        self._logger = glogging.get_logger(
            log_Name=self.get_client_name(),
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'INTERNAL/email_client'
        )

    def send_email(self, from_email, to_email, content, is_test=False):
        assert isinstance(content, str)
        request = EmailPRCRequest()
        request.is_test = is_test
        request.from_email = from_email
        request.to_email = to_email
        request.content = content
        self.send_request(request=request)
