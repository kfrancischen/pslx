from concurrent import futures
import grpc

from pslx.core.base import Base
from pslx.schema.rpc_pb2_grpc import add_GenericRPCServicer_to_server
from pslx.util.dummy_util import DummyUtil


class GenericServer(Base):

    def __init__(self, server_name):
        super().__init__()
        self._server_name = server_name
        self._logger = DummyUtil.dummy_logging()
        self._url = None
        self._rpc_server = None

    def create_server(self, max_worker, server_url):
        self._rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_worker))
        self._url = server_url

    def add_rpc(self, rpc):
        add_GenericRPCServicer_to_server(rpc, self._rpc_server)

    def start_server(self):
        self._rpc_server.add_insecure_port(self._url)
        self._rpc_server.start()
        self._rpc_server.wait_for_termination()
