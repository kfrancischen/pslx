import multiprocessing

from pslx.core.graph_base import GraphBase
import pslx.core.exception as exception
from pslx.core.operator_base import OperatorBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import Signal
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import ContainerSnapshot
from pslx.tool.filelock_tool import FileLockTool
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil
from pslx.util.dummy_util import DummyUtil


class ContainerBase(GraphBase):
    DATA_MODEL = DataModelType.DEFAULT
    STATUS = Status.IDLE

    def __init__(self, container_name, ttl=-1):
        super().__init__()
        self._container_name = container_name
        self._is_initialized = False
        self._snapshot_file_folder = FileUtil.join_paths_to_dir_with_mode(
            root_dir=FileUtil.join_paths_to_dir(root_dir=self.DATABASE_DIR, base_name='snapshots'),
            base_name=self.get_class_name() + '__' + container_name,
            ttl=ttl
        )
        self._start_time = None
        self._end_time = None
        self._logger = DummyUtil.dummy_logging()
        self._upstream_ops = []

    def initialize(self, force=False):
        for operator in self._node_name_to_node_dict.values():
            if operator.get_status() != self.STATUS:
                self.sys_log("Status of " + operator.get_node_name() + " is not consistent.")
                if not force:
                    raise exception.OperatorStatusInconsistentException
                else:
                    operator.set_status(self.STATUS)
            if operator.get_data_model() != self.DATA_MODEL:
                self.sys_log("Data model of " + operator.get_node_name() + " is not consistent.")
                if not force:
                    raise exception.OperatorDataModelInconsistentException
                else:
                    operator.get_data_model(self.DATA_MODEL)

        self._is_initialized = True

    def set_status(self, status):
        self.sys_log("Switching to " + ProtoUtil.get_name_by_value(enum_type=Status, value=status) +
                     " status from " + ProtoUtil.get_name_by_value(enum_type=Status, value=self.STATUS) + '.')
        self.STATUS = status

    def unset_status(self):
        self.set_status(status=Status.IDLE)

    def uninitialize(self):
        self._is_initialized = False

    def add_operator_edge(self, from_operator, to_operator):
        if self._is_initialized:
            self.sys_log("Cannot add more connections if the container is already initialized.")
            raise exception.ContainerAlreadyInitializedException
        self.add_direct_edge(from_node=from_operator, to_node=to_operator)

    def add_upstream_op(self, op_snapshot_file_pattern):
        self._upstream_ops.append(op_snapshot_file_pattern)

    def get_container_snapshot(self):
        if not self._is_initialized:
            self.sys_log("Warning: taking snapshot when the container is not initialized.")

        snapshot = ContainerSnapshot()
        snapshot.container_name = self._container_name
        snapshot.is_initialized = self._is_initialized
        snapshot.status = self.STATUS
        if self._start_time:
            snapshot.start_time = str(self._start_time)
        if self._end_time:
            snapshot.end_time = str(self._end_time)

        for op_name, op in self._node_name_to_node_dict.items():
            op_output_file = FileUtil.join_paths_to_file(
                root_dir=FileUtil.join_paths_to_dir(FileUtil.dir_name(self._snapshot_file_folder), 'operators'),
                base_name='SNAPSHOT_' + str(TimezoneUtil.cur_time_in_pst()) + '_' + op_name + '.pb'
            )
            snapshot.operator_snapshot_map[op_name].CopyFrom(op.get_operator_snapshot(output_file=op_output_file))

        self.sys_log("Saved to folder " + self._snapshot_file_folder + '.')
        self._logger.write_log("Saved to folder " + self._snapshot_file_folder + '.')
        output_file_name = FileUtil.join_paths_to_file(
            root_dir=FileUtil.dir_name(self._snapshot_file_folder),
            base_name='SNAPSHOT_' + str(TimezoneUtil.cur_time_in_pst()) + '_' + self._container_name + '.pb'
        )
        with FileLockTool(output_file_name, read_mode=False):
            FileUtil.write_proto_to_file(
                proto=snapshot,
                file_name=output_file_name
            )
        return snapshot

    def _execute(self, task_queue, finished_queue):
        for operator_name in iter(task_queue.get, Signal.STOP):
            self.sys_log("Starting task: " + operator_name)
            self._logger.write_log("Starting task: " + operator_name)
            op = self._node_name_to_node_dict[operator_name]
            op.execute()
            self.sys_log("Taking snapshot after executing " + operator_name)
            self.get_container_snapshot()
            finished_queue.put(operator_name)
            self.sys_log("Finished task: " + operator_name)
            self._logger.write_log("Finished task: " + operator_name)

    def execute(self, is_backfill=False, num_process=1):
        if not self._is_initialized:
            self.sys_log("Cannot execute if the container is not initialized.")
            raise exception.ContainerUninitializedException

        self.sys_log('Upstream operators are: ' + ', '.join(self._upstream_ops))
        unblocked_blocker = 0
        while unblocked_blocker < len(self._upstream_ops):
            for blocker in self._upstream_ops:
                latest_snapshot_files = FileUtil.get_file_names_from_pattern(pattern=blocker)
                if not latest_snapshot_files:
                    continue
                latest_snapshot_file = latest_snapshot_files[-1]
                if OperatorBase.get_status_from_snapshot(snapshot_file=latest_snapshot_file) != Status.SUCCEEDED:
                    continue
                else:
                    unblocked_blocker += 1

        self.get_container_snapshot()
        self.sys_log("Taking snapshot at start.")
        self.set_status(status=Status.RUNNING)
        operator_status = {}
        if is_backfill:
            operator_status = self._get_latest_status_of_operators()

        self._start_time = TimezoneUtil.cur_time_in_pst()
        task_queue, finished_queue, num_tasks = multiprocessing.Queue(), multiprocessing.Queue(), 0
        process_list = []
        for _ in range(num_process):
            process = multiprocessing.Process(target=self._execute, args=(task_queue, finished_queue))
            process.start()
            process_list.append(process)

        node_levels = self.get_node_levels()
        max_level = max(node_levels.keys())
        for level in range(max_level + 1):
            for operator_name in node_levels[level]:
                if is_backfill and operator_status[operator_name] == Status.SUCCEEDED:
                    self._node_name_to_node_dict[operator_name].set_status(status=Status.SUCCEEDED)
                    continue
                task_queue.put(operator_name)
                num_tasks += 1
        for _ in range(num_process):
            task_queue.put(Signal.STOP)

        for process in process_list:
            process.join()

        self._end_time = TimezoneUtil.cur_time_in_pst()

        self.set_status(status=Status.SUCCEEDED)
        for operator in self._node_name_to_node_dict.values():
            if operator.get_status() == Status.FAILED:
                self.set_status(status=Status.FAILED)
                break

        log_str = 'Finishing order is: '
        tasks_list = []
        for _ in range(num_tasks):
            tasks_list.append(finished_queue.get())

        log_str += ', '.join(tasks_list)
            
        self._logger.write_log(log_str)
        self.sys_log(log_str)

        self.get_container_snapshot()
        self.sys_log("Taking snapshot in the end.")

    def _get_latest_status_of_operators(self):
        operator_status = {}
        snapshot_files = FileUtil.get_file_names_in_dir(
            dir_name=FileUtil.join_paths_to_dir(FileUtil.dir_name(self._snapshot_file_folder), 'operators'))
        for snapshot_file in snapshot_files[::-1]:
            operator_name = snapshot_file.split('_')[1]
            if operator_name not in operator_status:
                operator_status[operator_name] = self._node_name_to_node_dict[operator_name].get_status_from_snapshot(
                    snapshot_file=snapshot_file
                )
            if len(operator_status) == len(self._node_name_to_node_dict):
                break
        return operator_status
