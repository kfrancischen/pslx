from collections import defaultdict
from queue import Queue
import threading
from galaxy_py import glogging

from pslx.core.graph_base import GraphBase
import pslx.core.exception as exception
from pslx.core.operator_base import OperatorBase
from pslx.micro_service.container_backend.client import ContainerBackendRPCClient
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import Signal
from pslx.schema.enums_pb2 import Status
from pslx.schema.snapshots_pb2 import ContainerSnapshot
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil
from pslx.util.dummy_util import DummyUtil


class ContainerBase(GraphBase):
    DATA_MODEL = DataModelType.DEFAULT

    def __init__(self, container_name, logger=DummyUtil.dummy_logger()):
        super().__init__()
        self._container_name = container_name
        self._is_initialized = False
        self._snapshot_file_folder = FileUtil.join_paths_to_dir(
            EnvUtil.get_pslx_env_variable(var='PSLX_SNAPSHOT_DIR'), self._container_name)
        self._start_time = None
        self._end_time = None
        self._logger = logger
        self._upstream_ops = []
        self._backend = None
        self._status = Status.IDLE
        self._counter = defaultdict(int)

    def get_container_name(self):
        return self._container_name

    def bind_backend(self, server_url):
        self._backend = ContainerBackendRPCClient(
            client_name=self._container_name + '_backend',
            server_url=server_url
        )
        self._logger.info("Bind to backend with name " + self._backend.get_client_name() + " at server url " +
                          server_url + '.')

    def initialize(self, force=False):
        for operator in self._node_name_to_node_dict.values():
            if operator.get_status() != self._status:
                self._logger.error("Status of [" + operator.get_node_name() + "] is not consistent for container ["
                                   + self.get_container_name() + '].')
                self._SYS_LOGGER.error("Status of [" + operator.get_node_name() + "] is not consistent for container ["
                                       + self.get_container_name() + '].')
                if not force:
                    raise exception.OperatorStatusInconsistentException(
                        "Status of [" + operator.get_node_name() + "] is not consistent for container ["
                        + self.get_container_name() + '].')
                else:
                    operator.set_status(self._status)
            if operator.get_data_model() != self.DATA_MODEL:
                self._logger.error("Data model of [" + operator.get_node_name() + "] is not consistent for container ["
                                   + self.get_container_name() + '].')
                self._SYS_LOGGER.error("Data model of [" + operator.get_node_name() + "] is not consistent for container ["
                                       + self.get_container_name() + '].')
                if not force:
                    raise exception.OperatorDataModelInconsistentException(
                        "Data model of [" + operator.get_node_name() + "] is not consistent for container ["
                        + self.get_container_name() + '].')
                else:
                    operator.set_data_model(self.DATA_MODEL)

        self._is_initialized = True

    def set_status(self, status):
        self._SYS_LOGGER.info('Container [' + self.get_container_name() + "] switching to [" +
                              ProtoUtil.get_name_by_value(enum_type=Status, value=status) +
                              "] status from [" + ProtoUtil.get_name_by_value(enum_type=Status, value=self._status) + '].')
        self._status = status

    def unset_status(self):
        self._SYS_LOGGER.info("Unset status for container [" + self.get_container_name() + '].')
        self.set_status(status=Status.IDLE)

    def uninitialize(self):
        self._is_initialized = False

    def add_operator_edge(self, from_operator, to_operator):
        if self._is_initialized:
            self._logger.error("Cannot add more connections if the container [" + self.get_container_name() +
                               "] is already initialized.")
            self._SYS_LOGGER.error("Cannot add more connections if the container [" + self.get_container_name() +
                                   "] is already initialized.")
            raise exception.ContainerAlreadyInitializedException(
                "Cannot add more connections if the container [" + self.get_container_name() +
                "] is already initialized.")
        self.add_direct_edge(from_node=from_operator, to_node=to_operator)
        from_operator.bind_to_container(container=self)
        to_operator.bind_to_container(container=self)

    def counter_increment(self, counter_name, n):
        self._counter[counter_name] = self._counter[counter_name] + n

    def unset_counters(self):
        self._counter.clear()

    def add_upstream_op(self, op_snapshot_file_pattern):
        self._upstream_ops.append(op_snapshot_file_pattern)

    def get_container_snapshot(self, send_backend=True):
        if not self._is_initialized:
            self._logger.error("Warning: taking snapshot when the container [" + self.get_container_name() +
                               "] is not initialized.")
            self._SYS_LOGGER.error("Warning: taking snapshot when the container [" + self.get_container_name() +
                                   "] is not initialized.")

        snapshot = ContainerSnapshot()
        snapshot.container_name = self._container_name
        snapshot.is_initialized = self._is_initialized
        snapshot.status = self._status
        snapshot.class_name = self.get_full_class_name()
        snapshot.mode = self._mode
        snapshot.data_model = self.DATA_MODEL
        snapshot.log_file = FileUtil.convert_local_to_cell_path(glogging.get_logger_file(self._logger))
        snapshot.run_cell = EnvUtil.get_other_env_variable(var='GALAXY_fs_cell', fallback_value='')
        for key, val in self._counter.items():
            snapshot.counters[key] = val
        if self._start_time:
            snapshot.start_time = str(self._start_time)
        if self._end_time:
            snapshot.end_time = str(self._end_time)

        for op_name, op in self._node_name_to_node_dict.items():
            if 'Dummy' in op.get_class_name():
                continue
            op_output_file = FileUtil.join_paths_to_file(
                root_dir=FileUtil.join_paths_to_dir(FileUtil.dir_name(
                    self._snapshot_file_folder), 'operators'),
                base_name=op_name + '_SNAPSHOT_' + str(TimezoneUtil.cur_time_in_pst()) + '.pb'
            )
            snapshot.operator_snapshot_map[op_name].CopyFrom(op.get_operator_snapshot(output_file=op_output_file))

        self._SYS_LOGGER.info("Snapshot saved to folder [" +
                              FileUtil.convert_local_to_cell_path(self._snapshot_file_folder) + '].')
        self._logger.info(
            "Snapshot saved to folder [" + FileUtil.convert_local_to_cell_path(self._snapshot_file_folder) + '].')
        output_file_name = FileUtil.join_paths_to_file(
            root_dir=FileUtil.join_paths_to_dir(FileUtil.dir_name(
                self._snapshot_file_folder), 'containers'),
            base_name=self._container_name + '_SNAPSHOT_' + str(TimezoneUtil.cur_time_in_pst()) + '.pb'
        )

        FileUtil.write_proto_to_file(
            proto=snapshot,
            file_name=output_file_name
        )
        if self._backend and send_backend:
            try:
                self._backend.send_to_backend(snapshot=snapshot)
            except Exception as err:
                self._logger.error("Sending backend failed with error " + str(err) + '.')

        return snapshot

    def _execute(self, task_queue, finished_queue):
        for operator_name in iter(task_queue.get, Signal.STOP):
            self._SYS_LOGGER.info(
                "Starting task [" + operator_name + "] for container [" + self.get_container_name() + '].')
            self._logger.info("Starting task [" + operator_name + "] for container [" +
                              self.get_container_name() + '].')
            op = self._node_name_to_node_dict[operator_name]
            op.execute()
            self._SYS_LOGGER.info("Taking snapshot after executing [" + operator_name + "] for container [" +
                                  self.get_container_name() + '].')
            self.get_container_snapshot(send_backend=op.allow_container_snapshot())
            finished_queue.put(operator_name)
            self._SYS_LOGGER.info("Finished task [" + operator_name + "] for container [" +
                                  self.get_container_name() + '].')
            self._logger.info("Finished task [" + operator_name + "] for container [" +
                              self.get_container_name() + '].')
            task_queue.task_done()
        task_queue.task_done()

    def execute(self, is_backfill=False, num_threads=1):
        if is_backfill:
            if self.DATA_MODEL == DataModelType.STREAMING:
                self._logger.warning('STREAMING container [' + self.get_container_name() +
                                     '] does not support backfill mode.')
                return
            self._SYS_LOGGER.debug("Running in backfill mode for container [" + self.get_container_name() + '].')
            self._logger.debug("Running in backfill mode for container [" + self.get_container_name() + '].')

        if not self._is_initialized:
            self._SYS_LOGGER.error("Cannot execute if the container [" + self.get_container_name() +
                                   "] is not initialized.")
            self._logger.error("Cannot execute if the container [" + self.get_container_name() +
                               "] is not initialized.")
            raise exception.ContainerUninitializedException(
                "Cannot execute if the container [" + self.get_container_name() + "] is not initialized.")

        self._SYS_LOGGER.info('Upstream operators are: ' + ', '.join(self._upstream_ops))
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

        self.set_status(status=Status.RUNNING)
        self._SYS_LOGGER.info("Taking snapshot of container [" + self.get_container_name() + "] at start.")
        self._logger.info("Taking snapshot of container [" + self.get_container_name() + "] at start.")
        self.get_container_snapshot()
        operator_status = {}
        if is_backfill:
            self._SYS_LOGGER.info("Updating the status of operators from previous snapshots.")
            operator_status = self._get_latest_status_of_operators()

        self._start_time = TimezoneUtil.cur_time_in_pst()
        task_queue, finished_queue, num_tasks = Queue(), Queue(), 0

        node_levels = self.get_node_levels()
        max_level = max(node_levels.keys())
        for level in range(max_level + 1):
            for operator_name in node_levels[level]:
                if is_backfill and operator_status[operator_name] == Status.SUCCEEDED:
                    self._node_name_to_node_dict[operator_name].set_status(status=Status.SUCCEEDED)
                    continue
                self._SYS_LOGGER.info("Adding operator [" + operator_name + "] to task queue for container [" +
                                      self.get_container_name() + '].')
                self._logger.info("Adding operator [" + operator_name + "] to task queue for container [" +
                                  self.get_container_name() + '].')
                task_queue.put(operator_name)
                num_tasks += 1

        for _ in range(num_threads):
            task_queue.put(Signal.STOP)

        if num_threads > 1:
            thread_list = []
            self._logger.info("Using [" + str(num_threads) + "] threads. Enter multi-threading mode.")
            for _ in range(num_threads):
                thread = threading.Thread(target=self._execute, args=(task_queue, finished_queue))
                thread.daemon = True
                thread.start()
                thread_list.append(thread)

            self._SYS_LOGGER.info("Joining all threads.")
            for thread in thread_list:
                thread.join()
            self._SYS_LOGGER.info("Done Joining all threads.")
        else:
            # this is mainly for streaming case.
            self._logger.info("Using only 1 thread. Execute the container directly.")
            self._execute(task_queue=task_queue, finished_queue=finished_queue)

        self._end_time = TimezoneUtil.cur_time_in_pst()

        self.set_status(status=Status.SUCCEEDED)
        for operator in self._node_name_to_node_dict.values():
            if operator.get_status() == Status.FAILED:
                self.set_status(status=Status.FAILED)
                break

        log_str = 'Tasks finishing order is: '
        tasks_list = []
        for _ in range(num_tasks):
            tasks_list.append(finished_queue.get())

        log_str += '-->'.join(tasks_list)

        self._logger.info(log_str + '.')
        self._SYS_LOGGER.info(log_str + '.')

        self.get_container_snapshot()
        self._SYS_LOGGER.info("Taking snapshot of container [" + self.get_container_name() + "] in the end.")
        self._logger.info("Taking snapshot of container [" + self.get_container_name() + "] in the end.")

    def _get_latest_status_of_operators(self):
        operator_status = {}
        snapshot_files = FileUtil.list_files_in_dir(
            dir_name=FileUtil.join_paths_to_dir(FileUtil.dir_name(self._snapshot_file_folder), 'operators'))
        for snapshot_file in snapshot_files[::-1]:
            operator_name = snapshot_file.split('/')[-1].split('_')[1]
            if operator_name not in operator_status:
                self._logger.info("Getting status for operator [" + operator_name + '].')
                self._SYS_LOGGER.info("Getting status for operator [" + operator_name + '].')
                operator_status[operator_name] = self._node_name_to_node_dict[operator_name].get_status_from_snapshot(
                    snapshot_file=snapshot_file
                )
                self._SYS_LOGGER.info("Status for operator [" + operator_name + '] is [' + ProtoUtil.get_name_by_value(
                    enum_type=Status, value=operator_status[operator_name]) + '].')
            if len(operator_status) == len(self._node_name_to_node_dict):
                break
        return operator_status
