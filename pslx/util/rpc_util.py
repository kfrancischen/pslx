import grpc
from galaxy_py import gclient
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import HealthCheckerRequest
from pslx.schema.rpc_pb2_grpc import GenericRPCServiceStub
from pslx.util.env_util import EnvUtil


class RPCUtil(object):
    @classmethod
    def check_health_and_qps(cls, server_url):
        request = HealthCheckerRequest()
        request.server_url = server_url
        timeout = int(EnvUtil.get_pslx_env_variable(var="PSLX_GRPC_TIMEOUT"))
        with grpc.insecure_channel(server_url) as channel:
            stub = GenericRPCServiceStub(channel=channel)
            try:
                response = stub.CheckHealth(
                    request=request,
                    metadata=[
                        (
                            "pslx_rpc_password",
                            EnvUtil.get_pslx_env_variable("PSLX_RPC_PASSWORD"),
                        )
                    ],
                    timeout=timeout,
                )
                return response.server_status, response.server_qps
            except Exception as _:
                return Status.FAILED, 0

    @classmethod
    def remote_execute(cls, cell, home_dir, binary, program_args, env_kargs):
        gclient.remote_execute(cell, home_dir, binary, program_args, env_kargs)


class EndpointUtil(object):
    @classmethod
    def get_exchange_info(cls, endpoint):
        assert endpoint.startwith("/pubsub") or endpoint.startwith("/mq"), "Endpoint should start with /pubsub or /mq."
        info = endpoint.split("/")
        assert (
            len(info) == 4
        ), "Endpoint should be in the format of /pubsub/$EXCHANGE/$TOPIC or /pubsub/$EXCHANGE/$TOPIC."
        return info[2], info[3]

    @classmethod
    def format_endpoint(cls, exchange, topic, endpoint_type='pubsub'):
        assert endpoint_type in ['pubsub', 'mq'], 'endpoint_type has to be one of pubsub or mq.'
        return '/'.join(['', endpoint_type, exchange, topic])
