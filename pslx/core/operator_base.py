import time
from pslx.core.exception import OperatorFailureException
from pslx.core.node_base import OrderedNodeBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import OperatorStatus
from pslx.schema.enums_pb2 import SortOrder
from pslx.schema.snapshots_pb2 import OperatorSnapShot
from pslx.util.proto_util import get_name_by_value, write_proto_to_file


class OperatorBase(OrderedNodeBase):
    DATA_MODEL = DataModelType.DEFAULT
    STATUS = OperatorStatus.IDLE

    def __init__(self, node_name, order=SortOrder.ORDER):
        super().__init__(node_name=node_name, order=order)
        self._slo = -1
        self._snapshot = OperatorSnapShot()

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
        self.log_print("Switching to " + get_name_by_value(enum_type=OperatorStatus, value=status) +
                       " status from " + get_name_by_value(enum_type=OperatorStatus, value=self.STATUS) + '.')
        self.STATUS = status

    def unset_status(self):
        self.set_status(status=OperatorStatus.IDLE)

    def get_status(self):
        return self.STATUS

    def wait_for_upstream_status(self):
        if self.DATA_MODEL != DataModelType.DEFAULT:
            for parent in self.get_parents_nodes():
                if parent.get_status() in [OperatorStatus.WAITING, OperatorStatus.RUNNING]:
                    self.log_print("Upstream operator " + parent.get_node_name() + " is still in status: " +
                                   get_name_by_value(enum_type=OperatorStatus, value=parent.get_status()) + '.')
                    return False
                elif parent.get_status() == OperatorStatus.FAILED:
                    self.set_status(status=OperatorStatus.FAILED)
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
            if (parent_node.get_status() in [OperatorStatus.IDLE, OperatorStatus.WAITING, OperatorStatus.RUNNING] and
                    self.STATUS in [OperatorStatus.RUNNING, OperatorStatus.SUCCEEDED, OperatorStatus.FAILED]):
                return False
            if (parent_node.get_status() == OperatorStatus.FAILED and
                    self.STATUS in [OperatorStatus.RUNNING, OperatorStatus.SUCCEEDED]):
                return False

        return True

    def take_snapshot(self, output_file):
        self._snapshot.mode_type = self.get_mode()
        self._snapshot.operator_name = self.get_node_name()
        self._snapshot.data_model = self.get_data_model()
        self._snapshot.status = self.get_status()
        self.log_print("Saved to file " + output_file + '.')
        write_proto_to_file(
            proto=self._snapshot,
            file_name=output_file
        )

    def execute(self, **kwargs):
        assert self.is_data_model_consistent() and self.is_status_consistent()
        for parent_node in self.get_parents_nodes():
            if parent_node.get_status() == OperatorStatus.FAILED:
                self.log_print("Operator " + parent_node.get_node_name() +
                               " failed. This results in failure of all the following descendant operators.")
                self.set_status(status=OperatorStatus.FAILED)
                return

        self.set_status(status=OperatorStatus.RUNNING)
        for child_node in self.get_children_nodes():
            if child_node.get_status() != OperatorStatus.WAITING:
                child_node.set_status(OperatorStatus.WAITING)
        if self._slo < 0:
            self.log_print("Operator has negative -1. This might result in indefinite run. "
                           "Please check set_slo() function.")
        start_time = time.time()
        while self._slo < 0 or time.time() - start_time > self._slo > 0:
            try:
                self._execute(**kwargs)
                self.set_status(status=OperatorStatus.SUCCEEDED)
            except OperatorFailureException as err:
                self.log_print(str(err))
                self.set_status(status=OperatorStatus.FAILED)

    def _execute(self, **kwargs):
        raise NotImplementedError
