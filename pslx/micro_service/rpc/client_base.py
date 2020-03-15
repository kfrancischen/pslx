import grpc

from pslx.core.base import Base
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceStub
from pslx.util.env_util import EnvUtil
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil


class ClientBase(Base):
    RESPONSE_MESSAGE_TYPE = None

    def __init__(self, client_name, server_url):
        super().__init__()
        self._client_name = client_name
        self._logger = DummyUtil.dummy_logging()
        self._server_url = server_url.replace('http://', '').replace('https://', '')
        self._channel = None

    def get_client_name(self):
        return self._client_name

    def get_server_url(self):
        return self._server_url

    @classmethod
    def get_response_type(cls):
        return cls.RESPONSE_MESSAGE_TYPE

    def send_request(self, request, root_certificate=None):
        generic_request = ProtoUtil.compose_generic_request(request=request)
        if self.RESPONSE_MESSAGE_TYPE:
            generic_request.message_type = ProtoUtil.infer_str_from_message_type(
                    message_type=self.RESPONSE_MESSAGE_TYPE
                )
        self._logger.write_log("Client getting request of uuid " + generic_request.uuid + '.')
        self.sys_log("Client getting request of uuid " + generic_request.uuid + '.')
        try:
            options = [
                ('grpc.max_receive_message_length', EnvUtil.get_pslx_env_variable(var='PSLX_GRPC_MAX_MESSAGE_LENGTH')),
                ('grpc.max_send_message_length', EnvUtil.get_pslx_env_variable(var='PSLX_GRPC_MAX_MESSAGE_LENGTH')),
            ]
            if not root_certificate:
                self._logger.write_log("Start with insecure channel.")
                with grpc.insecure_channel(self._server_url, options=options) as channel:
                    self._logger.write_log("Channel created with url " + self._server_url)
                    stub = GenericRPCServiceStub(channel=channel)
                    response = stub.SendRequest(request=generic_request)
            else:
                self._logger.write_log("Start with secure channel.")
                channel_credential = grpc.ssl_channel_credentials(root_certificate)
                with grpc.secure_channel(self._server_url, channel_credential, options=options) as channel:
                    self._logger.write_log("Channel created with url " + self._server_url)
                    stub = GenericRPCServiceStub(channel=channel)
                    response = stub.SendRequest(request=generic_request)

            if not self.RESPONSE_MESSAGE_TYPE:
                return None
            else:
                return ProtoUtil.any_to_message(
                    message_type=self.RESPONSE_MESSAGE_TYPE,
                    any_message=response.response_data
                )
        except Exception as err:
            self._logger.write_log(self.get_client_name() + " send request with error " + str(err) + '.')
            self.sys_log(self.get_client_name() + " send request with error " + str(err) + '.')
            pass
