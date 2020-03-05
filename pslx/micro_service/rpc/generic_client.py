import grpc

from pslx.core.base import Base
from pslx.schema.rpc_pb2_grpc import GenericRPCStub
from pslx.util.dummy_util import DummyUtil


class GenericClient(Base):

    def __init__(self, client_name):
        super().__init__()
        self._client_name = client_name
        self._logger = DummyUtil.dummy_logging()
        self._stub = None
        self._channel = None

    def create_client(self, server_url):
        self._channel = grpc.insecure_channel(server_url)
        self._stub = GenericRPCStub(channel=self._channel)

    def send_request(self, request, require_response=False):
        if require_response:
            return self._send_request_with_response(request=request)
        else:
            self._send_request_without_response(request=request)
            return None

    def _send_request_with_response(self, request):
        response = self._stub.RPCWithReturn(request=request)
        return response

    def _send_request_without_response(self, request):
        self._stub.RCPWithoutReturn(request=request)

    def close(self):
        self._channel.close()
