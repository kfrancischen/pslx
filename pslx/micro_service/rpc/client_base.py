import grpc
import uuid

from pslx.core.base import Base
from pslx.schema.rpc_pb2 import GenericRPCRequest
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceStub
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


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

    def send_request(self, request):
        generic_request = GenericRPCRequest()
        generic_request.request_data.CopyFrom(ProtoUtil.message_to_any(message=request))
        generic_request.timestamp = str(TimezoneUtil.cur_time_in_pst())
        generic_request.uuid = str(uuid.uuid4())
        self.sys_log("Getting request of uuid " + generic_request.uuid + '.')
        try:
            with grpc.insecure_channel(self._server_url) as channel:
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
