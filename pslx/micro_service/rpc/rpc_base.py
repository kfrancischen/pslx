from pslx.core.base import Base
from pslx.schema.common_pb2 import EmptyMessage
from pslx.schema.rpc_pb2 import GenericRPCResponse, GenericRPCRequestResponsePair
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceServicer
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class RPCBase(GenericRPCServiceServicer, Base):
    REQUEST_MESSAGE_TYPE = None

    def __init(self, service_name, request_storage=None):
        Base.__init__(self)
        self._logger = DummyUtil.dummy_logging()
        self._service_name = service_name
        self._request_storage = request_storage

    def SendRequest(self, request, context):
        decomposed_request = self.request_decomposer(request=request)
        response, status = self.impl(request=decomposed_request)
        generic_response = self.response_composer(response=response)
        generic_response.request_uuid = decomposed_request.uuid
        generic_response.status = status
        request.status = status

        if self._request_storage:
            request_response_pair = GenericRPCRequestResponsePair
            request_response_pair.uuid = request.uuid
            request_response_pair.generic_rpc_request.CopyFrom(request)
            request_response_pair.generic_rpc_response.CopyFrom(generic_response)

            self._request_storage.write(
                data=[request.uuid, request_response_pair],
                params={
                    'overwrite': True,
                }
            )

        return generic_response

    @classmethod
    def request_decomposer(cls, request):
        assert cls.REQUEST_MESSAGE_TYPE is not None
        return ProtoUtil.any_to_message(
            message_type=cls.REQUEST_MESSAGE_TYPE,
            any_message=request.request_data
        )

    @classmethod
    def response_composer(cls, response):
        generic_response = GenericRPCResponse()
        if not response:
            empty_message = EmptyMessage()
            generic_response.request_data.CopyFrom(ProtoUtil.message_to_any(empty_message))
            generic_response.timestamp = str(TimezoneUtil.cur_time_in_pst())
        else:
            generic_response.request_data.CopyFrom(ProtoUtil.message_to_any(response))
        generic_response.timestamp = str(TimezoneUtil.cur_time_in_pst())

        return generic_response

    def impl(self, request):
        raise NotImplementedError
