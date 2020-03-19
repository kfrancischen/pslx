from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import EmailPRCRequest
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil


class EmailRPCClient(ClientBase):

    def __init__(self, client_name, server_url):
        super().__init__(client_name=client_name, server_url=server_url)
        self._logger = LoggingTool(
            name=client_name,
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def send_email(self, from_email, to_email, content, is_test=False):
        assert isinstance(content, str)
        request = EmailPRCRequest()
        request.is_test = is_test
        request.from_email = from_email
        request.to_email = to_email
        request.content = content
        self.send_request(request=request)
