import os
from pslx.core.base import Base
from pslx.schema.enums_pb2 import StorageType
from pslx.schema.common_pb2 import EmptyMessage
from pslx.schema.rpc_pb2 import GenericRPCResponse, GenericRPCRequestResponsePair
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceServicer
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class RPCBase(GenericRPCServiceServicer, Base):
    REQUEST_MESSAGE_TYPE = None

    def __init__(self, service_name, rpc_storage=None):
        Base.__init__(self)
        self._logger = DummyUtil.dummy_logging()
        self._service_name = service_name
        if rpc_storage:
            assert rpc_storage.get_storage_type() == StorageType.PARTITIONER_STORAGE
            if 'ttl' not in rpc_storage.get_dir_name():
                self.sys_log("Warning. Please ttl the request log table.")
            underlying_storage = ProtoTableStorage()
            rpc_storage.set_underlying_storage(storage=underlying_storage)
            rpc_storage.set_max_size(max_size=os.getenv('PSLX_INTERNAL_CACHE', 100))
        self._rpc_storage = rpc_storage

    def get_rpc_service_name(self):
        return self._service_name

    def get_storage(self):
        return self._rpc_storage

    def SendRequest(self, request, context):
        self.sys_log("Get request with uuid " + request.uuid)
        decomposed_request = self.request_decomposer(request=request)
        response, status = self.send_request_impl(request=decomposed_request)
        generic_response = self.response_composer(response=response)
        generic_response.request_uuid = request.uuid
        generic_response.status = status
        request.status = status
        generic_response.message_type = request.message_type
        request.message_type = ProtoUtil.infer_str_from_message_type(
            message_type=self.REQUEST_MESSAGE_TYPE
        )

        if self._rpc_storage:
            request_response_pair = GenericRPCRequestResponsePair()
            request_response_pair.uuid = request.uuid
            request_response_pair.generic_rpc_request.CopyFrom(request)
            request_response_pair.generic_rpc_response.CopyFrom(generic_response)

            self._rpc_storage.write(
                data=[request.uuid, request_response_pair],
                params={
                    'overwrite': True,
                    'make_partition': True,
                }
            )
            self.sys_log("Save to " + self._rpc_storage.get_latest_dir())

        return generic_response

    @classmethod
    def get_request_message_type(cls):
        return cls.REQUEST_MESSAGE_TYPE

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
            generic_response.response_data.CopyFrom(ProtoUtil.message_to_any(empty_message))
            generic_response.timestamp = str(TimezoneUtil.cur_time_in_pst())
        else:
            generic_response.response_data.CopyFrom(ProtoUtil.message_to_any(response))
        generic_response.timestamp = str(TimezoneUtil.cur_time_in_pst())

        return generic_response

    def send_request_impl(self, request):
        raise NotImplementedError
