from pslx.core.base import Base
from pslx.schema.rpc_pb2_grpc import GenericRPCServicer
from pslx.util.dummy_util import DummyUtil


class RPCBase(GenericRPCServicer, Base):

    def __init(self, service_name):
        Base.__init__(self)
        self._logger = DummyUtil.dummy_logging()
        self._service_name = service_name

    def reply_with_return(self, request, context):
        decomposed_request = self.request_decomposer(request=request)
        response = self.impl(decomposed_request=decomposed_request)
        return self.response_composer(response=response)

    def reply_without_return(self, request, context):
        decomposed_request = self.request_decomposer(request=request)
        self.impl(decomposed_request=decomposed_request)

    def request_decomposer(self, request):
        raise NotImplementedError

    def response_composer(self, response):
        raise NotImplementedError

    def impl(self, decomposed_request):
        raise NotImplementedError
