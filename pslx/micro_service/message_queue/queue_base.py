from pslx.micro_service.rpc.rpc_base import RPCBase


class QueueBase(RPCBase):
    REQUEST_MESSAGE_TYPE = None

    def __init__(self, queue_name, queue_storage=None):
        RPCBase.__init__(self, service_name=queue_name, rpc_storage=queue_storage)

    def get_queue_name(self):
        return self.get_rpc_service_name()

    def send_request(self, request):
        return self.SendRequest(request=request, context=None)

    def get_response_and_status_impl(self, request):
        raise NotImplementedError
