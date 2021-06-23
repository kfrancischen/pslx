from concurrent import futures
import grpc
from galaxy_py import glogging

from pslx.core.base import Base
from pslx.core.exception import RPCAlreadyExistException, RPCServerNotInitializedException
from pslx.schema.rpc_pb2_grpc import add_GenericRPCServiceServicer_to_server
from pslx.util.env_util import EnvUtil


class GenericServer(Base):

    def __init__(self, server_name):
        super().__init__()
        self._server_name = server_name
        self._logger = glogging.get_logger(
            log_name=self.get_server_name(),
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'INTERNAL/generic_server'
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
        self._SYS_LOGGER.info("Create server with num of workers = " + str(max_worker) + " and url = " + server_url +
                              ' for server [' + self.get_server_name() + '].')
        self._logger.info("Create server with num of workers = " + str(max_worker) + " and url = " + server_url +
                          ' for server [' + self.get_server_name() + '].')
        self._rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_worker))
        self._url = server_url

    def bind_rpc(self, rpc):
        if self._has_added_rpc:
            self._SYS_LOGGER.error("RPC already exist for server [" + self.get_server_name() +
                                   "], cannot bind any more.")
            self._logger.error("RPC already exist for server [" + self.get_server_name() + "], cannot bind any more.")
            raise RPCAlreadyExistException("RPC already exist for server [" + self.get_server_name() +
                                           "], cannot bind any more.")
        self._SYS_LOGGER.info("Server " + self._url + " binding to server [" + rpc.get_rpc_service_name() + '].')
        self._logger.info("Server " + self._url + " binding to server [" + rpc.get_rpc_service_name() + '].')
        add_GenericRPCServiceServicer_to_server(rpc, self._rpc_server)
        self._has_added_rpc = True

    def start_server(self):
        if self._rpc_server:
            self._logger.info("Starting server.")
            self._rpc_server.add_insecure_port(self._url)

            self._rpc_server.start()
            self._rpc_server.wait_for_termination()
        else:
            self._logger.error("Please create server for " + self.get_server_name() + " first.")
            self._SYS_LOGGER.error("Please create server for " + self.get_server_name() + " first.")
            raise RPCServerNotInitializedException("Please create server for " + self.get_server_name() + " first.")
