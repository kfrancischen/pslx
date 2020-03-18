from pslx.core.base import Base
from pslx.schema.enums_pb2 import StorageType
from pslx.schema.rpc_pb2 import GenericRPCRequestResponsePair
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceServicer
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.util.env_util import EnvUtil
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil


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
            rpc_storage.set_max_capacity(max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE'))
        self._rpc_storage = rpc_storage

    def get_rpc_service_name(self):
        return self._service_name

    def get_storage(self):
        return self._rpc_storage

    def SendRequest(self, request, context):
        self.sys_log("rpc getting request with uuid " + request.uuid + '.')
        self._logger.info("rpc getting request with uuid " + request.uuid + '.')
        decomposed_request = self.request_decomposer(request=request)
        response, status = self.get_response_and_status_impl(request=decomposed_request)
        generic_response = ProtoUtil.compose_generic_response(response=response)
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
            self.sys_log("Request response pair saved to " + self._rpc_storage.get_latest_dir() + '.')

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

    def get_response_and_status_impl(self, request):
        raise NotImplementedError
