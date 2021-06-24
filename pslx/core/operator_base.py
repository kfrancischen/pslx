import time
from galaxy_py import glogging
from pslx.core.base import DummyLogger
from pslx.core.exception import OperatorFailureException, FileNotExistException
from pslx.core.node_base import OrderedNodeBase
from pslx.schema.enums_pb2 import DataModelType, Status, Signal
from pslx.schema.snapshots_pb2 import OperatorSnapshot
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil, TimeSleepObj


class OperatorBase(OrderedNodeBase):
    DATA_MODEL = DataModelType.DEFAULT
    CONTENT_MESSAGE_TYPE = None

    def __init__(self, operator_name, logger=DummyLogger()):
        super().__init__(node_name=operator_name)
        self._config = {
            'save_snapshot': False,
            'allow_container_snapshot': False,
            'allow_failure': False,
        }
        self._start_time = None
        self._end_time = None
        self._persistent = False
        self._status = Status.IDLE
        self._container = None
        self._logger = logger

    def allow_container_snapshot(self):
        return self._config['allow_container_snapshot']

    def set_data_model(self, model):
        self._SYS_LOGGER.info("Switching to [" + ProtoUtil.get_name_by_value(enum_type=DataModelType, value=model) +
                              "] model from [" + ProtoUtil.get_name_by_value(
                                  enum_type=DataModelType, value=self.DATA_MODEL)
                              + '].')
        self.DATA_MODEL = model

    def unset_data_model(self):
        self.set_data_model(model=DataModelType.DEFAULT)

    def get_data_model(self):
        return self.DATA_MODEL

    def set_status(self, status):
        self._SYS_LOGGER.info('Node [' + self._node_name + "] switching to [" + ProtoUtil.get_name_by_value(
            enum_type=Status, value=status) + "] status from [" +
            ProtoUtil.get_name_by_value(enum_type=Status, value=self._status) + '].')
        self._status = status

    def unset_status(self):
        self._SYS_LOGGER.info("Unset status.")
        self.set_status(status=Status.IDLE)

    def unset_dependency(self):
        self._SYS_LOGGER.info("Unset dependencies")
        for child_node in self.get_children_nodes():
            self.delete_child(child_node=child_node)
        for parent_node in self.get_parents_nodes():
            self.delete_parent(parent_node=parent_node)

    def get_status(self):
        return self._status

    def bind_to_container(self, container):
        self._container = container

    def counter_increment(self, counter_name):
        self.counter_increment_by_n(counter_name=counter_name, n=1)

    def counter_increment_by_n(self, counter_name, n):
        self._container.counter_increment(counter_name=self.get_node_name() + ':' + counter_name, n=n)

    def mark_as_done(self):
        self._SYS_LOGGER.info("Mark [" + self.get_node_name() + '] as done.')
        self._logger.info("Mark [" + self.get_node_name() + '] as done.')
        self.set_status(status=Status.SUCCEEDED)

    def mark_as_persistent(self):
        self._persistent = True

    def is_done(self):
        return self._status == Status.SUCCEEDED

    def get_content_from_dependency(self, dependency_name):
        if not self.get_parent(parent_name=dependency_name):
            return None
        else:
            return self.get_parent(parent_name=dependency_name).get_content()

    @classmethod
    def get_status_from_snapshot(cls, snapshot_file):
        try:
            snapshot = FileUtil.read_proto_from_file(
                proto_type=OperatorSnapshot,
                file_name=FileUtil.die_if_file_not_exist(file_name=snapshot_file)
            )
            return snapshot.status
        except FileNotExistException as _:
            return Status.IDLE

    @classmethod
    def get_content_from_snapshot(cls, snapshot_file, message_type):
        try:
            snapshot = FileUtil.read_proto_from_file(
                proto_type=OperatorSnapshot,
                file_name=FileUtil.die_if_file_not_exist(file_name=snapshot_file)
            )
            return ProtoUtil.any_to_message(message_type=message_type, any_message=snapshot.content)
        except FileNotExistException as _:
            return message_type()

    def wait_for_upstream_status(self):
        unfinished_op = []
        if self.DATA_MODEL != DataModelType.DEFAULT:
            for parent in self.get_parents_nodes():
                if parent.get_status() in [Status.IDLE, Status.WAITING, Status.RUNNING]:
                    self._SYS_LOGGER.info("Upstream operator [" + parent.get_node_name() + "] is still in status [" +
                                          ProtoUtil.get_name_by_value(enum_type=Status, value=parent.get_status()) +
                                          '].')
                    self._logger.info("Upstream operator [" + parent.get_node_name() + "] is still in status [" +
                                      ProtoUtil.get_name_by_value(enum_type=Status, value=parent.get_status()) + '].')
                    unfinished_op.append(parent.get_node_name())
                elif parent.get_status() == Status.FAILED:
                    self._SYS_LOGGER.info("Upstream operator [" + parent.get_node_name() + "] failed.")
                    # streaming mode allows failure from its dependencies.
                    if not self._config['allow_failure']:
                        self._SYS_LOGGER.error('This results in failure of all the following descendant operators.')
                        self._logger.error('This results in failure of all the following descendant operators.')
                        self.set_status(status=Status.FAILED)
                        unfinished_op = []
                        break
                    else:
                        self._SYS_LOGGER.warning("Failure is allowed in Streaming mode. The rest of operators will" +
                                                 " continue.")
                        self._logger.warning('This results in failure of all the following descendant operators.')

        return unfinished_op

    def is_data_model_consistent(self):
        for parent_node in self.get_parents_nodes():
            if parent_node.get_data_model() != self.DATA_MODEL:
                return False
        return True

    def is_status_consistent(self):
        for parent_node in self.get_parents_nodes():
            if (parent_node.get_status() in [Status.IDLE, Status.WAITING, Status.RUNNING] and
                    self._status in [Status.RUNNING, Status.SUCCEEDED, Status.FAILED]):
                return False
            if (not self._config['allow_failure'] and parent_node.get_status() == Status.FAILED and
                    self._status in [Status.RUNNING, Status.SUCCEEDED]):
                return False

        return True

    def get_operator_snapshot(self, output_file=None):
        snapshot = OperatorSnapshot()
        snapshot.operator_name = self.get_node_name()
        snapshot.data_model = self.get_data_model()
        snapshot.status = self.get_status()
        snapshot.node_snapshot.CopyFrom(self.get_node_snapshot())
        snapshot.class_name = self.get_full_class_name()
        snapshot.log_file = glogging.get_logger_file(self._logger)
        if self._start_time:
            snapshot.start_time = str(self._start_time)
        if self.get_status() == Status.SUCCEEDED and self._end_time:
            snapshot.end_time = str(self._end_time)
        if self._persistent:
            assert self.CONTENT_MESSAGE_TYPE is not None
            snapshot.content.CopyFrom(ProtoUtil.message_to_any(message=self._content))
        if output_file and self._config['save_snapshot'] and 'Dummy' not in self.get_class_name():
            self._SYS_LOGGER.info("Saved to file " + output_file + '.')

            FileUtil.write_proto_to_file(
                proto=snapshot,
                file_name=output_file
            )
        return snapshot

    def execute(self):
        assert self.is_data_model_consistent() and self.is_status_consistent()
        if self.get_status() == Status.SUCCEEDED:
            self._SYS_LOGGER.info("Operator [" + self.get_node_name() +
                                  "] already succeeded. It might have been finished by another process.")
            self._logger.info("Operator [" + self.get_node_name() +
                              "] already succeeded. It might have been finished by another process.")
            return

        unfinished_parent_ops = self.wait_for_upstream_status()
        while unfinished_parent_ops:
            time.sleep(TimeSleepObj.ONE_SECOND)
            self._SYS_LOGGER.info("Waiting for parent process to finish: " + ','.join(unfinished_parent_ops) + '.')
            self._logger.info("Waiting for parent process to finish: " + ','.join(unfinished_parent_ops) + '.')
            unfinished_parent_ops = self.wait_for_upstream_status()

        if self.get_status() == Status.FAILED:
            self._SYS_LOGGER.info("Operator [" + self.get_node_name() +
                                  "] already failed because of upstream jobs. Existing...")
            self._logger.error("Operator [" + self.get_node_name() +
                               "] already failed because of upstream jobs. Existing...", publish=True)
            return

        self.set_status(status=Status.RUNNING)
        self._start_time = TimezoneUtil.cur_time_in_pst()
        for child_node in self.get_children_nodes():
            if child_node.get_status() != Status.WAITING:
                child_node.set_status(Status.WAITING)
        if self._config['allow_container_snapshot']:
            self._SYS_LOGGER.info("Taking snapshot when status is updated.")
            self._container.get_container_snapshot()
        try:
            if self._execute_impl() == Signal.STOP:
                self._end_time = TimezoneUtil.cur_time_in_pst()
                self.set_status(status=Status.SUCCEEDED)
        except OperatorFailureException as err:
            self._SYS_LOGGER.error("Execute operator [" + self.get_node_name() + "] with error " + str(err) + '.')
            self._logger.error("Execute operator [" + self.get_node_name() + "] with error " + str(err) + '.')
            self.set_status(status=Status.FAILED)

    def _execute_impl(self):
        try:
            self.execute_impl()
            return Signal.STOP
        except Exception as err:
            self._SYS_LOGGER.error("operator [" + self.get_node_name() + "] failed with error " + str(err) + '.')
            self._logger.error("operator [" + self.get_node_name() + "] failed with error " + str(err) + '.',
                               publish=True)
            raise OperatorFailureException("operator [" + self.get_node_name() + "] failed with error " +
                                           str(err) + '.')

    def execute_impl(self):
        raise NotImplementedError
