import grpc
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import HealthCheckerRequest
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceStub
from pslx.util.file_util import FileUtil


class RPCUtil(object):

    @classmethod
    def check_health(cls, server_url, root_certificate_path=None):
        request = HealthCheckerRequest()
        request.server_url = server_url
        if not root_certificate_path:
            request.secure = False
            with grpc.insecure_channel(server_url) as channel:
                stub = GenericRPCServiceStub(channel=channel)
                try:
                    response = stub.CheckHealth(request=request)
                    return response.server_status
                except Exception as _:
                    return Status.FAILED
        else:
            with open(FileUtil.die_if_file_not_exist(file_name=root_certificate_path), 'r') as infile:
                root_certificate = infile.read()
            request.secure = True
            channel_credential = grpc.ssl_channel_credentials(root_certificate)
            with grpc.secure_channel(server_url, channel_credential) as channel:
                stub = GenericRPCServiceStub(channel=channel)
                try:
                    response = stub.CheckHealth(request=request)
                    return response.server_status
                except Exception as _:
                    return Status.FAILED
