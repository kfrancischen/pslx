from pslx.core.graph_base import GraphBase
from pslx.core.exception import ContainerUninitializedException, ContainerAlreadyInitializedException
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import Status
from pslx.util.file_util import FileUtil


class ContainerBase(GraphBase):
    DATA_MODEL = DataModelType.DEFAULT
    STATUS = Status.IDLE

    def __init__(self, container_name, tmp_file_folder='tmp/', ttl=-1):
        super().__init__()
        self._container_name = container_name
        self._operator_edge_to_dependency_map = dict()
        self._is_initialized = False
        self._tmp_file_folder = FileUtil.join_paths(
            root_dir=tmp_file_folder,
            class_name=self.get_class_name(),
            ttl=ttl
        )

    def initialize(self):
        # More to be added (TODO)
        self._is_initialized = True

    def uninitialize(self):
        self._is_initialized = False

    def add_operator_edge(self, from_operator, to_operator):
        if self._is_initialized:
            self.log_print("Cannot add more connections if the container is already initialized.")
            raise ContainerAlreadyInitializedException
        self.add_direct_edge(from_node=from_operator, to_node=to_operator)

    def get_container_snapshot(self):
        if not self._is_initialized:
            self.log_print("Cannot take snapshot if the container is not initialized.")
            raise ContainerUninitializedException
        # (TODO)
        pass

    def execute(self):
        if not self._is_initialized:
            self.log_print("Cannot execute if the container is not initialized.")
            raise ContainerUninitializedException
        # (TODO)
        pass
