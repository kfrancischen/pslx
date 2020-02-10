from pslx.core.graph_base import GraphBase
import pslx.core.exception as exception
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import ContainerSnapshot
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import write_proto_to_file


class ContainerBase(GraphBase):
    DATA_MODEL = DataModelType.DEFAULT
    STATUS = Status.IDLE

    def __init__(self, container_name, tmp_file_folder='tmp/', ttl=-1):
        super().__init__()
        self._container_name = container_name
        self._is_initialized = False
        self._tmp_file_folder = FileUtil.join_paths(
            root_dir=tmp_file_folder,
            class_name=self.get_class_name() + '__' + container_name,
            ttl=ttl
        )

    def initialize(self, force=False):
        for operator in self._node_name_to_node_dict.values():
            if operator.set_status() != self.STATUS:
                self.log_print("Status of " + operator.get_node_name() + " is not consistent.")
                if not force:
                    raise exception.OperatorStatusInconsistentException
                else:
                    operator.set_status(self.STATUS)
            if operator.get_data_model() != self.DATA_MODEL:
                self.log_print("Data model of " + operator.get_node_name() + " is not consistent.")
                if not force:
                    raise exception.OperatorDataModelInconsistentException
                else:
                    operator.get_data_model(self.DATA_MODEL)

        # More to be added (TODO)
        self._is_initialized = True

    def uninitialize(self):
        self._is_initialized = False

    def add_operator_edge(self, from_operator, to_operator):
        if self._is_initialized:
            self.log_print("Cannot add more connections if the container is already initialized.")
            raise exception.ContainerAlreadyInitializedException
        self.add_direct_edge(from_node=from_operator, to_node=to_operator)

    def get_container_snapshot(self, output_file=None):
        if not self._is_initialized:
            self.log_print("Warning: taking snapshot when the container is not initialized.")

        snapshot = ContainerSnapshot()
        snapshot.container_name = self._container_name
        snapshot.is_initialized = self._is_initialized
        snapshot.status = self.STATUS
        for op_name, op in self._node_name_to_node_dict:
            if output_file:
                op_output_file = FileUtil.dir_name(output_file) + '/' + op_name + '.pb'
            else:
                op_output_file = None

            snapshot.operator_snapshot_map[op_name] = op.get_operator_snapshot(output_file=op_output_file)

        if output_file:
            self.log_print("Saved to file " + output_file + '.')
            write_proto_to_file(
                proto=snapshot,
                file_name=output_file
            )
        return snapshot

    def execute(self):
        if not self._is_initialized:
            self.log_print("Cannot execute if the container is not initialized.")
            raise exception.ContainerUninitializedException
        # (TODO)
        pass
