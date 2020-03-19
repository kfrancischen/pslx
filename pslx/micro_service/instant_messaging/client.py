from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import InstantMessagingRPCRequest
from pslx.schema.enums_pb2 import InstantMessagingType
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil


class InstantMessagingRPCClient(ClientBase):
    RESPONSE_MESSAGE_TYPE = None
    INSTANT_MESSAGING_TYPE = None

    def __init__(self, channel_name, webhook_url, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)
        self._webhook_url = webhook_url
        self._channel_name = channel_name
        self._logger = LoggingTool(
            name=channel_name,
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )

    def get_channel_name(self):
        return self._channel_name

    def get_webhook_url(self):
        return self._webhook_url

    def send_message(self, message, is_test=False, root_certificate=None):
        assert isinstance(message, str)
        request = InstantMessagingRPCRequest()
        request.is_test = is_test
        request.type = self.INSTANT_MESSAGING_TYPE
        request.channel_name = self._channel_name
        request.webhook_url = self._webhook_url
        request.message = message
        self.send_request(request=request, root_certificate=root_certificate)


class SlackClient(InstantMessagingRPCClient):
    INSTANT_MESSAGING_TYPE = InstantMessagingType.SLACK


class RocketchatClient(InstantMessagingRPCClient):
    INSTANT_MESSAGING_TYPE = InstantMessagingType.ROCKETCHAT


class TeamsClient(InstantMessagingRPCClient):
    INSTANT_MESSAGING_TYPE = InstantMessagingType.TEAMS

