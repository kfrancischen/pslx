import datetime
import time

from pslx.core.exception import StorageReadException, StorageWriteException
from pslx.core.node_base import OrderedNodeBase
from pslx.core.tree_base import TreeBase
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType, SortOrder, Status
from pslx.storage.default_storage import DefaultStorage
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.storage.storage_base import StorageBase
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimeSleepObj, TimezoneUtil


class PartitionerBase(StorageBase):
    STORAGE_TYPE = StorageType.PARTITIONER_STORAGE
    PARTITIONER_TYPE = None
    PARTITIONER_TYPE_TO_HEIGHT_MAP = {
        PartitionerStorageType.YEARLY: 1,
        PartitionerStorageType.MONTHLY: 2,
        PartitionerStorageType.DAILY: 3,
        PartitionerStorageType.HOURLY: 4,
        PartitionerStorageType.MINUTELY: 5,
    }

    def __init__(self, logger=None, max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')):
        super().__init__(logger=logger)
        self._file_tree = None
        self._max_capacity = int(max_capacity)
        self._underlying_storage = DefaultStorage(logger=logger)

    def get_partitioner_type(self):
        return self.PARTITIONER_TYPE

    def set_underlying_storage(self, storage):
        assert storage.STORAGE_TYPE != StorageType.PARTITIONER_STORAGE
        self._underlying_storage = storage

    def set_max_capacity(self, max_capacity):
        self._max_capacity = int(max_capacity)

    def initialize_from_file(self, file_name):
        self.sys_log("Initialize_from_file function is not implemented for storage type "
                     + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        pass

    def initialize_from_dir(self, dir_name, force=False):

        def _recursive_initialize_from_dir(node, max_recursion):
            self.sys_log("Starting recursion of " + str(max_recursion) + '.')
            if max_recursion == 0:
                self.sys_log("Exhausted all recursions.")
                self._logger.info("Exhausted all recursions.")
                return

            node_name = node.get_node_name()
            for child_node_name in sorted(FileUtil.list_dirs_in_dir(dir_name=node_name), reverse=from_scratch):
                if from_scratch and self._file_tree.get_num_nodes() >= self._max_capacity > 0:
                    self.sys_log("Reach the max number of node: " + str(self._max_capacity))
                    return

                newly_added_string = child_node_name.replace(node_name, '').replace('/', '')
                if not newly_added_string.isdigit():
                    continue

                if not from_scratch and self._cmp_dir_by_timestamp(
                        dir_name_1=child_node_name, dir_name_2=self.get_latest_dir()):
                    continue

                child_node = self._file_tree.find_node(node_name=child_node_name)
                if not child_node:
                    child_node = OrderedNodeBase(
                        node_name=child_node_name
                    )
                    # The nodes are ordered from large to small. So if the tree is built scratch, since the directory
                    # is listed from large to small, SortOrder.ORDER is used. If it is incremental build, since the
                    # directory is listed from small to large, SortOrder.REVERSE is used.
                    order = SortOrder.ORDER if from_scratch else SortOrder.REVERSE
                    self._file_tree.add_node(
                        parent_node=node,
                        child_node=child_node,
                        order=order
                    )
                    self.sys_log("Adding new node with name " + child_node_name + node.get_node_name() + '.')
                    self._logger.info("Adding new node with name " + child_node_name + " to parent node "
                                      + node.get_node_name() + '.')

                    if not from_scratch:
                        self._file_tree.trim_tree(max_capacity=self._max_capacity)

                _recursive_initialize_from_dir(node=child_node, max_recursion=max_recursion - 1)

        from_scratch = False
        dir_name = FileUtil.normalize_dir_name(dir_name=dir_name)
        FileUtil.create_dir_if_not_exist(dir_name=dir_name)
        if not self._file_tree or self.is_updated() or force:
            root_node = OrderedNodeBase(
                node_name=FileUtil.normalize_dir_name(dir_name=dir_name)
            )
            self._file_tree = TreeBase(root=root_node, max_dict_size=self._max_capacity)
            from_scratch = True

        _recursive_initialize_from_dir(
            node=self._file_tree.get_root_node(),
            max_recursion=self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]
        )

    def set_config(self, config):
        self._underlying_storage.set_config(config=config)

    def get_dir_name(self):
        if self._file_tree:
            return self._file_tree.get_root_name()
        else:
            return None

    def get_size(self):
        if not self._file_tree:
            return 0
        else:
            return self._file_tree.get_num_nodes()

    def is_empty(self):
        if self.is_updated():
            self.sys_log("Tree updated, need force rebuilding the tree.")
            self._logger.info("Tree updated, need force rebuilding the tree.")
            self.initialize_from_dir(dir_name=self.get_dir_name(), force=True)

        rightmost_leaf_name = self._file_tree.get_rightmost_leaf()
        if FileUtil.is_dir_empty(dir_name=rightmost_leaf_name):
            return True
        else:
            return False

    def is_updated(self):
        rightmost_leaf_name = self._file_tree.get_rightmost_leaf()
        if not FileUtil.does_dir_exist(dir_name=rightmost_leaf_name):
            return True
        else:
            return False

    def _cmp_dir_by_timestamp(self, dir_name_1, dir_name_2):
        dir_name_1 = dir_name_1.replace(self._file_tree.get_root_name(), '')
        dir_name_2 = dir_name_2.replace(self._file_tree.get_root_name(), '')
        if not dir_name_2:
            return False
        else:
            dir_name_1 = FileUtil.normalize_dir_name(dir_name=dir_name_1)
            dir_name_2 = FileUtil.normalize_dir_name(dir_name=dir_name_2)
            dir_name_1_split, dir_name_2_split = dir_name_1.split('/')[:-1], dir_name_2.split('/')[:-1]
            if len(dir_name_1_split) > len(dir_name_2_split):
                return False

            dir_name_2 = FileUtil.normalize_dir_name('/'.join(dir_name_2_split[:len(dir_name_1_split)]))
            dir_name_1_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=dir_name_1)
            dir_name_2_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=dir_name_2)
            return dir_name_1_timestamp < dir_name_2_timestamp

    def get_latest_dir(self):
        if self.is_empty():
            self.sys_log("Current partitioner is empty.")
            return ''
        else:
            return self._file_tree.get_leftmost_leaf()

    def get_oldest_dir(self):
        if self.is_empty():
            self.sys_log("Current partitioner is empty.")
            return ''
        else:
            return self._file_tree.get_rightmost_leaf()

    def get_oldest_dir_in_root_directory(self):
        if self.is_empty():
            self.sys_log("Current partitioner is empty.")
            return ''
        else:
            oldest_directory = self._file_tree.get_root_name()
            while True:
                sub_dirs = FileUtil.list_dirs_in_dir(dir_name=oldest_directory)
                if sub_dirs:
                    oldest_directory = sorted(sub_dirs)[0]
                else:
                    return oldest_directory

    def get_previous_dir(self, cur_dir):
        cur_dir = cur_dir.replace(self._file_tree.get_root_name(), '')
        cur_time = FileUtil.parse_dir_to_timestamp(dir_name=cur_dir)
        if self.PARTITIONER_TYPE == PartitionerStorageType.YEARLY:
            pre_time = datetime.datetime(cur_time.year - 1, 1, 1)
        elif self.PARTITIONER_TYPE == PartitionerStorageType.MONTHLY:
            if cur_time.month == 1:
                pre_time = datetime.datetime(cur_time.year - 1, 12, 1)
            else:
                pre_time = datetime.datetime(cur_time.year, cur_time.month - 1, 1)
        elif self.PARTITIONER_TYPE == PartitionerStorageType.DAILY:
            pre_time = cur_time - datetime.timedelta(days=1)
        elif self.PARTITIONER_TYPE == PartitionerStorageType.HOURLY:
            pre_time = cur_time - datetime.timedelta(hours=1)
        else:
            pre_time = cur_time - datetime.timedelta(minutes=1)
        last_dir_name = FileUtil.parse_timestamp_to_dir(timestamp=pre_time).split('/')
        last_dir_name = '/'.join(last_dir_name[:self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]])
        last_dir_name = FileUtil.join_paths_to_dir(
            root_dir=self._file_tree.get_root_name(),
            base_name=last_dir_name
        )
        if FileUtil.does_dir_exist(dir_name=last_dir_name):
            return last_dir_name
        else:
            return None

    def get_next_dir(self, cur_dir):
        cur_dir = cur_dir.replace(self._file_tree.get_root_name(), '')
        cur_time = FileUtil.parse_dir_to_timestamp(dir_name=cur_dir)
        if self.PARTITIONER_TYPE == PartitionerStorageType.YEARLY:
            next_time = datetime.datetime(cur_time.year + 1, 1, 1)
        elif self.PARTITIONER_TYPE == PartitionerStorageType.MONTHLY:
            if cur_time.month == 12:
                next_time = datetime.datetime(cur_time.year + 1, 1, 1)
            else:
                next_time = datetime.datetime(cur_time.year, cur_time.month + 1, 1)
        elif self.PARTITIONER_TYPE == PartitionerStorageType.DAILY:
            next_time = cur_time + datetime.timedelta(days=1)
        elif self.PARTITIONER_TYPE == PartitionerStorageType.HOURLY:
            next_time = cur_time + datetime.timedelta(hours=1)
        else:
            next_time = cur_time + datetime.timedelta(minutes=1)

        next_dir_name = FileUtil.parse_timestamp_to_dir(timestamp=next_time).split('/')
        next_dir_name = '/'.join(next_dir_name[:self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]])

        next_dir_name = FileUtil.join_paths_to_dir(
            root_dir=self._file_tree.get_root_name(),
            base_name=next_dir_name
        )
        if FileUtil.does_dir_exist(dir_name=next_dir_name):
            return next_dir_name
        else:
            return None

    def _reinitialize_underlying_storage(self, file_base_name):
        file_name = FileUtil.join_paths_to_file(root_dir=self.get_latest_dir(), base_name=file_base_name)
        if not FileUtil.does_file_exist(file_name):
            self.sys_log("The file to read does not exist.")
            return
        self._underlying_storage.initialize_from_file(file_name=file_name)

    def read(self, params=None):
        if self._underlying_storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
            file_base_name = 'data.pb'
        else:
            file_base_name = 'data'
        if params and 'base_name' in params:
            file_base_name = params['base_name']
            params.pop('base_name', None)
        if params and 'reinitialize_underlying_storage' in params:
            self._reinitialize_underlying_storage(file_base_name=file_base_name)

        while self._writer_status != Status.IDLE:
            self.sys_log("Waiting for writer to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._reader_status = Status.RUNNING
        self.initialize_from_dir(dir_name=self._file_tree.get_root_name())
        if self.is_empty():
            self.sys_log("Current partitioner is empty, cannot read anything.")
            return []

        self.sys_log("Read from the latest partition.")
        latest_dir = self.get_latest_dir()
        file_name = FileUtil.join_paths_to_file(root_dir=latest_dir, base_name=file_base_name)
        if not FileUtil.does_file_exist(file_name):
            self.sys_log("The file to read does not exist.")
            raise StorageReadException

        if file_name != self._underlying_storage.get_file_name():
            self.sys_log("Sync to the latest file to " + file_name)
            self._underlying_storage.initialize_from_file(file_name=file_name)
        try:
            result = self._underlying_storage.read(params=params)
            self._reader_status = Status.IDLE
            return result
        except Exception as err:
            self.sys_log(str(err) + '.')
            self._logger.error(str(err) + '.')
            raise StorageReadException

    def read_range(self, params):

        def _reformat_time(timestamp):
            if self.PARTITIONER_TYPE == PartitionerStorageType.YEARLY:
                timestamp = timestamp.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0,
                                              tzinfo=None)
            elif self.PARTITIONER_TYPE == PartitionerStorageType.MONTHLY:
                timestamp = timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            elif self.PARTITIONER_TYPE == PartitionerStorageType.DAILY:
                timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            elif self.PARTITIONER_TYPE == PartitionerStorageType.HOURLY:
                timestamp = timestamp.replace(minute=0, second=0, microsecond=0, tzinfo=None)
            else:
                timestamp = timestamp.replace(second=0, microsecond=0, tzinfo=None)
            return timestamp

        assert 'start_time' in params and 'end_time' in params and params['start_time'] <= params['end_time']
        while self._writer_status != Status.IDLE:
            self.sys_log("Waiting for writer to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._reader_status = Status.RUNNING
        self.initialize_from_dir(dir_name=self._file_tree.get_root_name())
        if self.is_empty():
            self.sys_log("Current partitioner is empty, cannot read anything.")
            return {}

        oldest_dir, latest_dir = self.get_oldest_dir(), self.get_latest_dir()
        oldest_dir = oldest_dir.replace(self._file_tree.get_root_name(), '')
        latest_dir = latest_dir.replace(self._file_tree.get_root_name(), '')

        oldest_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=oldest_dir)
        latest_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=latest_dir)
        start_time = max(_reformat_time(params['start_time']), oldest_timestamp)
        end_time = min(_reformat_time(params['end_time']), latest_timestamp)
        result = {}
        try:
            while start_time <= end_time:
                dir_list = FileUtil.parse_timestamp_to_dir(timestamp=start_time).split('/')
                dir_name = '/'.join(dir_list[:self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]])
                dir_name = FileUtil.join_paths_to_dir(
                    root_dir=self._file_tree.get_root_name(),
                    base_name=dir_name
                )
                if FileUtil.does_dir_exist(dir_name=dir_name):
                    if self._underlying_storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
                        storage = ProtoTableStorage()
                    else:
                        storage = DefaultStorage()
                    file_names = FileUtil.list_files_in_dir(dir_name=dir_name)
                    for file_name in file_names:
                        storage.initialize_from_file(file_name=file_name)
                        if storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
                            result[file_name] = storage.read_all()
                        else:
                            result[file_name] = storage.read(params={
                                'num_line': -1
                            })

                if self.PARTITIONER_TYPE == PartitionerStorageType.YEARLY:
                    start_time = start_time.replace(year=start_time.year + 1, month=1, day=1)
                elif self.PARTITIONER_TYPE == PartitionerStorageType.MONTHLY:
                    if start_time.month == 12:
                        start_time = start_time.replace(year=start_time.year + 1, month=1, day=1)
                    else:
                        start_time = start_time.replace(month=start_time.month + 1)
                elif self.PARTITIONER_TYPE == PartitionerStorageType.DAILY:
                    start_time += datetime.timedelta(days=1)
                elif self.PARTITIONER_TYPE == PartitionerStorageType.HOURLY:
                    start_time += datetime.timedelta(hours=1)
                else:
                    start_time += datetime.timedelta(minutes=1)

            self._reader_status = Status.IDLE
            return result
        except Exception as err:
            self.sys_log("Read got exception " + str(err) + '.')
            self._logger.error("Read got exception " + str(err) + '.')
            raise StorageReadException

    def make_new_partition(self, timestamp):
        new_dir_list = FileUtil.parse_timestamp_to_dir(timestamp=timestamp).split('/')
        new_dir = '/'.join(new_dir_list[:self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]])
        child_node = OrderedNodeBase(
            node_name=FileUtil.join_paths_to_dir(
                root_dir=self._file_tree.get_root_name(),
                base_name=new_dir
            )
        )
        if FileUtil.does_dir_exist(dir_name=child_node.get_node_name()):
            self.sys_log(child_node.get_node_name() + " exist. Don't make new partition.")
            return None
        else:
            self.sys_log(child_node.get_node_name() + " doesn't exist. Make new partition.")
            self._logger.info(child_node.get_node_name() + " doesn't exist. Make new partition.")
            FileUtil.create_dir_if_not_exist(dir_name=child_node.get_node_name())
            self.initialize_from_dir(dir_name=self._file_tree.get_root_name())
            return child_node.get_node_name()

    def write(self, data, params=None):
        to_make_partition = True
        if params and 'make_partition' in params:
            to_make_partition = params['make_partition']
            params.pop('make_partition', None)

        if self._underlying_storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
            file_base_name = 'data.pb'
        else:
            file_base_name = 'data'
        if params and 'base_name' in params:
            file_base_name = params['base_name']
            params.pop('base_name', None)

        while self._reader_status != Status.IDLE:
            self.sys_log("Waiting for reader to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._writer_status = Status.RUNNING

        if to_make_partition:
            self.make_new_partition(timestamp=TimezoneUtil.cur_time_in_pst())

        self.initialize_from_dir(dir_name=self._file_tree.get_root_name())

        file_name = FileUtil.join_paths_to_file(
            root_dir=self._file_tree.get_leftmost_leaf(),
            base_name=file_base_name)

        if file_name != self._underlying_storage.get_file_name():
            self.sys_log("Sync to the latest file to " + file_name)
            self._underlying_storage.initialize_from_file(
                file_name=file_name
            )

        try:
            self._underlying_storage.write(data=data, params=params)
            self._writer_status = Status.IDLE
        except Exception as err:
            self.sys_log("Write got exception " + str(err) + '.')
            self._logger.error("Write got exception " + str(err) + '.')
            raise StorageWriteException

    def print_self(self):
        # for debug only
        if self._file_tree:
            self._file_tree.print_self()
