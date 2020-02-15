import datetime

from pslx.core.exception import OperatorFailureException, FileNotExistException
from pslx.core.node_base import OrderedNodeBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import SortOrder
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import OperatorSnapshot
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class OperatorBase(OrderedNodeBase):
    DATA_MODEL = DataModelType.DEFAULT
    STATUS = Status.IDLE

    def __init__(self, node_name, order=SortOrder.ORDER):
        super().__init__(node_name=node_name, order=order)
        self._config = {
            'save_snapshot': False,
            'slo': -1,
        }
        self._start_time = None
        self._end_time = None
        self._data = None
    
    def set_config(self, config):
        assert isinstance(config, dict)
        self._config.update(config)

    def set_data_model(self, model):
        self.log_print("Switching to " + ProtoUtil.get_name_by_value(enum_type=DataModelType, value=model) +
                       " model from " + ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) +
                       '.')
        self.DATA_MODEL = model

    def unset_data_model(self):
        self.set_data_model(model=DataModelType.DEFAULT)

    def get_data_model(self):
        return self.DATA_MODEL

    def set_status(self, status):
        self.log_print(self._node_name + " switching to " + ProtoUtil.get_name_by_value(enum_type=Status, value=status)
                       + " status from " + ProtoUtil.get_name_by_value(enum_type=Status, value=self.STATUS) + '.')
        self.STATUS = status

    def unset_status(self):
        self.set_status(status=Status.IDLE)

    def get_status(self):
        return self.STATUS

    def mark_as_done(self):
        self.set_status(status=Status.SUCCEEDED)

    def get_status_from_snapshot(self, snapshot_file):
        try:
            snapshot = FileUtil.read_proto_from_file(
                proto_type=OperatorSnapshot,
                file_name=FileUtil.die_if_not_exist(file_name=snapshot_file)
            )
            return snapshot.status
        except FileNotExistException as err:
            self.log_print(str(err))
            return Status.IDLE

    def get_data(self):
        return self._data

    def wait_for_upstream_status(self):
        if self.DATA_MODEL != DataModelType.DEFAULT:
            for parent in self.get_parents_nodes():
                if parent.get_status() in [Status.WAITING, Status.RUNNING]:
                    self.log_print("Upstream operator " + parent.get_node_name() + " is still in status: " +
                                   ProtoUtil.get_name_by_value(enum_type=Status, value=parent.get_status()) + '.')
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
        snapshot.slo = self._config['slo']
        if self._start_time:
            snapshot.start_time = str(self._start_time)
        if self._end_time:
            snapshot.end_time = str(self._end_time)
        if output_file and self._config['save_snapshot']:
            self.log_print("Saved to file " + output_file + '.')
            FileUtil.write_proto_to_file(
                proto=snapshot,
                file_name=output_file
            )
        return snapshot

    def execute(self):
        assert self.is_data_model_consistent() and self.is_status_consistent()
        if self.get_status() == Status.SUCCEEDED:
            self.log_print("Process already succeeded. It might have been finished by another process.")
            return
        for parent_node in self.get_parents_nodes():
            if parent_node.get_status() == Status.FAILED:
                self.log_print("Operator " + parent_node.get_node_name() +
                               " failed. This results in failure of all the following descendant operators.")
                self.set_status(status=Status.FAILED)
                return

        self.set_status(status=Status.RUNNING)
        self._start_time = TimezoneUtil.cur_time_in_pst()
        for child_node in self.get_children_nodes():
            if child_node.get_status() != Status.WAITING:
                child_node.set_status(Status.WAITING)
        if self._config['slo'] < 0:
            self.log_print("Operator has negative SLO = -1. This might result in indefinite run. "
                           "Please check set_slo() function.")

        while self._config['slo'] < 0 or (self._config['slo'] > 0 and TimezoneUtil.cur_time_in_pst() -
                                          self._start_time > datetime.timedelta(self._config['slo'])):
            try:
                if self._execute():
                    self._end_time = TimezoneUtil.cur_time_in_pst()
                    self.set_status(status=Status.SUCCEEDED)
                    return
            except OperatorFailureException as err:
                self.log_print(str(err))

        self.set_status(status=Status.FAILED)

    def _execute(self):
        raise NotImplementedError
