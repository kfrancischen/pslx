import time
from pslx.core.exception import OperatorFailureException
from pslx.core.node_base import OrderedNodeBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import SortOrder
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import OperatorSnapshot
from pslx.util.proto_util import get_name_by_value, write_proto_to_file


class OperatorBase(OrderedNodeBase):
    DATA_MODEL = DataModelType.DEFAULT
    STATUS = Status.IDLE

    def __init__(self, node_name, order=SortOrder.ORDER):
        super().__init__(node_name=node_name, order=order)
        self._slo = -1

    def set_slo(self, slo):
        self._slo = slo

    def set_data_model(self, model):
        self.log_print("Switching to " + get_name_by_value(enum_type=DataModelType, value=model) +
                       " model from " + get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + '.')
        self.DATA_MODEL = model

    def unset_data_model(self):
        self.set_data_model(model=DataModelType.DEFAULT)

    def get_data_model(self):
        return self.DATA_MODEL

    def set_status(self, status):
        self.log_print("Switching to " + get_name_by_value(enum_type=Status, value=status) +
                       " status from " + get_name_by_value(enum_type=Status, value=self.STATUS) + '.')
        self.STATUS = status

    def unset_status(self):
        self.set_status(status=Status.IDLE)

    def get_status(self):
        return self.STATUS

    def wait_for_upstream_status(self):
        if self.DATA_MODEL != DataModelType.DEFAULT:
            for parent in self.get_parents_nodes():
                if parent.get_status() in [Status.WAITING, Status.RUNNING]:
                    self.log_print("Upstream operator " + parent.get_node_name() + " is still in status: " +
                                   get_name_by_value(enum_type=Status, value=parent.get_status()) + '.')
                    return False
                elif parent.get_status() == Status.FAILED:
                    self.set_status(status=Status.FAILED)
                    self.log_print("Upstream operator " + parent.get_node_name() + " failed.")
                    break
        return True

    def is_data_model_consistent(self):
        for parent_node in self.get_parents_nodes():
            if parent_node.get_data_model() != self.DATA_MODEL:
                return False
        return True

    def is_status_consistent(self):
        for parent_node in self.get_parents_nodes():
            if (parent_node.get_status() in [Status.IDLE, Status.WAITING, Status.RUNNING] and
                    self.STATUS in [Status.RUNNING, Status.SUCCEEDED, Status.FAILED]):
                return False
            if (parent_node.get_status() == Status.FAILED and
                    self.STATUS in [Status.RUNNING, Status.SUCCEEDED]):
                return False

        return True

    def get_operator_snapshot(self, output_file=None):
        snapshot = OperatorSnapshot()
        snapshot.operator_name = self.get_node_name()
        snapshot.data_model = self.get_data_model()
        snapshot.status = self.get_status()
        snapshot.node_snapshot.CopyFrom(self.get_node_snapshot())
        if output_file:
            self.log_print("Saved to file " + output_file + '.')
            write_proto_to_file(
                proto=snapshot,
                file_name=output_file
            )
        return snapshot

    def execute(self, **kwargs):
        assert self.is_data_model_consistent() and self.is_status_consistent()
        for parent_node in self.get_parents_nodes():
            if parent_node.get_status() == Status.FAILED:
                self.log_print("Operator " + parent_node.get_node_name() +
                               " failed. This results in failure of all the following descendant operators.")
                self.set_status(status=Status.FAILED)
                return

        self.set_status(status=Status.RUNNING)
        for child_node in self.get_children_nodes():
            if child_node.get_status() != Status.WAITING:
                child_node.set_status(Status.WAITING)
        if self._slo < 0:
            self.log_print("Operator has negative SLO = -1. This might result in indefinite run. "
                           "Please check set_slo() function.")
        start_time = time.time()
        while self._slo < 0 or time.time() - start_time > self._slo > 0:
            try:
                self._execute(**kwargs)
                self.set_status(status=Status.SUCCEEDED)
            except OperatorFailureException as err:
                self.log_print(str(err))
                self.set_status(status=Status.FAILED)

    def _execute(self, **kwargs):
        raise NotImplementedError
