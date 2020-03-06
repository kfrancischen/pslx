import grpc
import uuid

from pslx.core.base import Base
from pslx.core.exception import RPCChannelCloseException
from pslx.schema.rpc_pb2 import GenericRPCRequest
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceStub
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class ClientBase(Base):
    RESPONSE_MESSAGE_TYPE = None

    def __init__(self, client_name):
        super().__init__()
        self._client_name = client_name
        self._logger = DummyUtil.dummy_logging()
        self._stub = None
        self._channel = None

    def get_client_name(self):
        return self._client_name

    def create_client(self, server_url):
        self._logger.write_log("Client created with url " + server_url)
        self._channel = grpc.insecure_channel(server_url)
        self._stub = GenericRPCServiceStub(channel=self._channel)

    def send_request(self, request):
        generic_request = GenericRPCRequest()
        generic_request.request_data.CopyFrom(ProtoUtil.message_to_any(message=request))
        generic_request.timestamp = str(TimezoneUtil.cur_time_in_pst())
        generic_request.uuid = str(uuid.uuid4())
        response = self._stub.SendRequest(request=generic_request)
        if not self.RESPONSE_MESSAGE_TYPE:
            return None
        else:
            return ProtoUtil.any_to_message(
                message_type=self.RESPONSE_MESSAGE_TYPE,
                any_message=response.request_data
            )

    def close(self):
        if self._channel:
            try:
                self._channel.close()
            except Exception as err:
                self.sys_log("Close channel with error " + str(err) + '.')
                raise RPCChannelCloseException
