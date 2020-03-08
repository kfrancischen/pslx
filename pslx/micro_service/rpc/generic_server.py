from concurrent import futures
import grpc
import os

from pslx.core.base import Base
from pslx.core.exception import RPCAlreadyExistException, RPCServerNotInitializedException
from pslx.schema.rpc_pb2_grpc import add_GenericRPCServiceServicer_to_server
from pslx.tool.logging_tool import LoggingTool


class GenericServer(Base):

    def __init__(self, server_name):
        super().__init__()
        self._server_name = server_name
        self._logger = LoggingTool(
            name=self.get_server_name(),
            ttl=os.getenv('PSLX_INTERNAL_TTL', 7)
        )
        self._url = None
        self._rpc_server = None
        self._has_added_rpc = False

    def get_server_name(self):
        return self._server_name

    def get_server_url(self):
        return self._url

    def create_server(self, max_worker, server_url):
        server_url = server_url.replace('http://', '').replace('https://', '')
        self.sys_log("Create server with num of workers = " + str(max_worker) + " and url = " + server_url + '.')
        self._logger.write_log("Create server with num of workers = " + str(max_worker) + " and url = " +
                               server_url + '.')
        self._rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_worker))
        self._url = server_url

    def bind_rpc(self, rpc):
        if self._has_added_rpc:
            self.sys_log("RPC already exist, cannot bind any more.")
            self._logger.write_log("RPC already exist, cannot bind any more.")
            raise RPCAlreadyExistException
        self.sys_log("Server " + self._url + " binding to rpc with name " + rpc.get_rpc_service_name() + '.')
        add_GenericRPCServiceServicer_to_server(rpc, self._rpc_server)
        self._has_added_rpc = True

    def start_server(self):
        if self._rpc_server:
            self._logger.write_log("Starting server.")
            self._rpc_server.add_insecure_port(self._url)
            self._rpc_server.start()
            self._rpc_server.wait_for_termination()
        else:
            self._logger.write_log("Please create server first.")
            self.sys_log("Please create server first.")
            raise RPCServerNotInitializedException
