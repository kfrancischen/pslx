import time

from pslx.core.exception import StorageReadException, StorageWriteException
from pslx.core.node_base import OrderedNodeBase
from pslx.core.tree_base import TreeBase
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType, SortOrder, Status
from pslx.storage.default_storage import DefaultStorage
from pslx.storage.storage_base import StorageBase
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

    def __init__(self, logger=None, max_size=-1):
        super().__init__(logger=logger)
        self._file_tree = None
        self._max_size = max_size
        self._underlying_storage = DefaultStorage(logger=logger)

    def get_partitioner_type(self):
        return self.PARTITIONER_TYPE

    def set_underlying_storage(self, storage):
        assert storage.STORAGE_TYPE != StorageType.PARTITIONER_STORAGE
        self._underlying_storage = storage

    def initialize_from_file(self, file_name):
        self.sys_log("Initialize_from_file function is not implemented for storage type "
                     + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        pass

    def initialize_from_dir(self, dir_name):
        dir_name = FileUtil.normalize_dir_name(dir_name=dir_name)

        def _recursive_initialize_from_dir(node, max_recursion):
            if max_recursion == 0:
                self.sys_log("Exhausted all recursions")
                self._logger.write_log("Exhausted all recursions")
                return

            node_name = node.get_node_name()
            for child_node_name in sorted(FileUtil.list_dirs_in_dir(dir_name=node_name), reverse=from_scratch):
                if from_scratch and self._file_tree.get_num_nodes() >= self._max_size > 0:
                    self.sys_log("Reach the max number of node: " + str(self._max_size))
                    return

                if not from_scratch and self._cmp_dir_by_timestamp(
                        dir_name_1=child_node_name, dir_name_2=self.get_latest_dir()):
                    continue

                child_node = self._file_tree.find_node(node_name=child_node_name)
                if not child_node:
                    child_node = OrderedNodeBase(
                        node_name=child_node_name,
                        order=SortOrder.REVERSE
                    )
                    self._file_tree.add_node(
                        parent_node=node,
                        child_node=child_node
                    )
                    self.sys_log("Adding new node with name " + child_node_name)
                    self._logger.write_log("Adding new node with name " + child_node_name)

                if not from_scratch:
                    self._file_tree.trim_tree(max_capacity=self._max_size)

                _recursive_initialize_from_dir(node=child_node, max_recursion=max_recursion - 1)

        from_scratch = False
        FileUtil.create_dir_if_not_exist(dir_name=dir_name)
        if not self._file_tree:
            root_node = OrderedNodeBase(
                node_name=FileUtil.normalize_dir_name(dir_name=dir_name),
                order=SortOrder.REVERSE
            )
            self._file_tree = TreeBase(root=root_node, max_dict_size=self._max_size)
            from_scratch = True

        _recursive_initialize_from_dir(
            node=self._file_tree.get_root_node(),
            max_recursion=self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]
        )

    def set_config(self, config):
        self._underlying_storage.set_config(config=config)

    def get_size(self):
        if not self._file_tree:
            return 0
        else:
            return self._file_tree.get_num_nodes()

    def is_empty(self):
        leftmost_leaf_name = self._file_tree.get_leftmost_leaf()
        if FileUtil.is_dir_empty(dir_name=leftmost_leaf_name):
            return True
        else:
            return False

    def _cmp_dir_by_timestamp(self, dir_name_1, dir_name_2):
        if not dir_name_2:
            return False
        else:
            dir_name_1 = dir_name_1.replace(self._file_tree.get_root_name(), '')
            dir_name_2 = dir_name_2.replace(self._file_tree.get_root_name(), '')

            dir_name_1_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=dir_name_1)
            dir_name_2_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=dir_name_2)
            return dir_name_1_timestamp < dir_name_2_timestamp

    def get_latest_dir(self):
        if self.is_empty():
            self.sys_log("Current partitioner is empty.")
            return ''
        else:
            return self._file_tree.get_rightmost_leaf()

    def get_oldest_dir(self):
        if self.is_empty():
            self.sys_log("Current partitioner is empty.")
            return ''
        else:
            return self._file_tree.get_leftmost_leaf()

    def read(self, params=None):
        if self._underlying_storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
            file_base_name = 'data.pb'
        else:
            file_base_name = 'data'
        if params and 'base_name' in params:
            file_base_name = params['base_name']
            params.pop('base_name', None)

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
            self.sys_log(str(err))
            self._logger.write_log(str(err))
            raise StorageReadException

    def _make_new_partition(self, timestamp):
        new_dir_list = FileUtil.parse_timestamp_to_dir(timestamp=timestamp).split('/')
        new_dir = '/'.join(new_dir_list[:self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]])
        child_node = OrderedNodeBase(
            node_name=FileUtil.join_paths_to_dir(
                root_dir=self._file_tree.get_root_name(),
                base_name=new_dir
            ),
            order=SortOrder.REVERSE
        )
        if FileUtil.does_dir_exist(dir_name=child_node.get_node_name()):
            self.sys_log(child_node.get_node_name() + " exist. Don't make new partition.")
            return None
        else:
            self.sys_log(child_node.get_node_name() + " doesn't exist. Make new partition.")
            self._logger.write_log(child_node.get_node_name() + " doesn't exist. Make new partition.")
            parent_node = self._file_tree.find_node(node_name=self._file_tree.get_rightmost_leaf())
            self._file_tree.add_node(
                parent_node=parent_node,
                child_node=child_node
            )
            self._file_tree.trim_tree(max_capacity=self._max_size)
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
            self._make_new_partition(timestamp=TimezoneUtil.cur_time_in_pst())

        self.initialize_from_dir(dir_name=self._file_tree.get_root_name())

        file_name = FileUtil.join_paths_to_file(
            root_dir=self._file_tree.get_rightmost_leaf(),
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
            self.sys_log(str(err))
            self._logger.write_log(str(err))
            raise StorageWriteException

    def print_self(self):
        # for debug only
        if self._file_tree:
            self._file_tree.print_self()
