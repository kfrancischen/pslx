import datetime
from galaxy_py import gclient_ext
from pslx.core.exception import StorageReadException, StorageWriteException
from pslx.core.node_base import OrderedNodeBase
from pslx.core.tree_base import TreeBase
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType, SortOrder
from pslx.schema.storage_pb2 import ProtoTable
from pslx.storage.default_storage import DefaultStorage
from pslx.storage.storage_base import StorageBase
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


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
        assert storage.STORAGE_TYPE not in [StorageType.PARTITIONER_STORAGE, StorageType.SHARDED_PROTO_TABLE_STORAGE]
        self._underlying_storage = storage

    def set_max_capacity(self, max_capacity):
        self._max_capacity = int(max_capacity)

    def initialize_from_file(self, file_name):
        self._SYS_LOGGER.fatal("Initialize_from_file function is not implemented for storage type ["
                               + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '].')
        pass

    def initialize_from_dir(self, dir_name, force=False):

        def _recursive_initialize_from_dir(node, max_recursion):
            self._SYS_LOGGER.info("Starting recursion of " + str(max_recursion) + '.')
            if max_recursion == 0:
                self._SYS_LOGGER.info("Exhausted all recursions for dir [" + dir_name + '].')
                self._logger.info("Exhausted all recursions for dir [" + dir_name + '].')
                return

            node_name = node.get_node_name()
            for child_node_name in sorted(FileUtil.list_dirs_in_dir(dir_name=node_name), reverse=from_scratch):
                if from_scratch and self._file_tree.get_num_nodes() >= self._max_capacity > 0:
                    self._SYS_LOGGER.info("Reach the max number of node: " + str(self._max_capacity) + '.')
                    return

                newly_added_string = child_node_name.replace(node_name, '').replace('/', '')
                if not newly_added_string.isdigit():
                    continue

                if not from_scratch and self._cmp_dir_by_timestamp(
                        dir_name_1=child_node_name, dir_name_2=self._get_latest_dir_internal()):
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
                    self._SYS_LOGGER.info("Adding new node [" + child_node_name + node.get_node_name() + '].')
                    self._logger.info("Adding new node [" + child_node_name + "] to parent node ["
                                      + node.get_node_name() + '].')

                    if not from_scratch:
                        self._file_tree.trim_tree(max_capacity=self._max_capacity)

                _recursive_initialize_from_dir(node=child_node, max_recursion=max_recursion - 1)

        from_scratch = False
        dir_name = FileUtil.normalize_dir_name(dir_name=dir_name)
        if not self._file_tree or force:
            # FileUtil.create_dir_if_not_exist(dir_name=dir_name)
            root_node = OrderedNodeBase(
                node_name=dir_name
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
        leftmost_leaf_name, rightmost_leaf_name = (self._file_tree.get_leftmost_leaf(),
                                                   self._file_tree.get_rightmost_leaf())
        if FileUtil.is_dir_empty(dir_name=leftmost_leaf_name) and FileUtil.is_dir_empty(dir_name=rightmost_leaf_name):
            return True
        else:
            return False

    def is_gabage_collected(self):
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

    def get_dir_in_timestamp(self, dir_name):
        dir_name = dir_name.replace(self._file_tree.get_root_name(), '')
        if dir_name:
            return FileUtil.parse_dir_to_timestamp(dir_name=dir_name)
        else:
            return None

    def _get_latest_dir_internal(self):
        if not self._file_tree:
            return ''
        else:
            return self._file_tree.get_leftmost_leaf()

    def _get_oldest_dir_in_root_directory_interal(self):
        if not self._file_tree:
            self._SYS_LOGGER.info("Current partitioner is empty.")
            return ''
        else:
            oldest_directory = self._file_tree.get_root_name()
            while True:
                sub_dirs = FileUtil.list_dirs_in_dir(dir_name=oldest_directory)
                if sub_dirs:
                    oldest_directory = sorted(sub_dirs)[0]
                else:
                    return oldest_directory

    def get_latest_dir(self):
        self.initialize_from_dir(dir_name=self.get_dir_name())
        if self.is_empty():
            self._SYS_LOGGER.info("Current partitioner is empty.")
            return ''
        else:
            return self._file_tree.get_leftmost_leaf()

    def get_oldest_dir(self):
        self.initialize_from_dir(dir_name=self.get_dir_name())
        if self.is_empty():
            self._SYS_LOGGER.info("Current partitioner is empty.")
            return ''
        else:
            return self._file_tree.get_rightmost_leaf()

    def get_oldest_dir_in_root_directory(self):
        self.initialize_from_dir(dir_name=self.get_dir_name())
        if self.is_empty():
            self._SYS_LOGGER.info("Current partitioner is empty.")
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
        self.initialize_from_dir(dir_name=self.get_dir_name())
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
        self.initialize_from_dir(dir_name=self.get_dir_name())
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
        file_name = FileUtil.join_paths_to_file(root_dir=self._get_latest_dir_internal(), base_name=file_base_name)
        if not FileUtil.does_file_exist(file_name):
            self._SYS_LOGGER.info("The file to read does not exist.")
            return
        self._underlying_storage.initialize_from_file(file_name=file_name)

    def read(self, params=None):
        self.initialize_from_dir(dir_name=self.get_dir_name())
        if self._underlying_storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
            file_base_name = 'data.pb'
        else:
            file_base_name = 'data'
        if params and 'base_name' in params:
            file_base_name = params['base_name']
            params.pop('base_name', None)
        if params and 'reinitialize_underlying_storage' in params:
            self._reinitialize_underlying_storage(file_base_name=file_base_name)

        self._SYS_LOGGER.info("Read from the latest partition.")
        latest_dir = self._get_latest_dir_internal()
        if not latest_dir:
            self._SYS_LOGGER.info("Current partitioner is empty, cannot read anything.")
            return []

        file_name = FileUtil.join_paths_to_file(root_dir=latest_dir, base_name=file_base_name)

        try:
            if file_name != self._underlying_storage.get_file_name():
                self._SYS_LOGGER.info("Sync to the latest file to " + file_name)
                self._underlying_storage.initialize_from_file(file_name=file_name)
            result = self._underlying_storage.read(params=params)
            return result
        except Exception as err:
            self._SYS_LOGGER.error("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            raise StorageReadException("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')

    def read_range(self, params):
        self.initialize_from_dir(dir_name=self.get_dir_name())

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

        oldest_dir, latest_dir = self._get_oldest_dir_in_root_directory_interal(), self._get_latest_dir_internal()
        if not latest_dir or not oldest_dir:
            self._logger.warning("Current partitioner [" + self.get_dir_name() +
                                 "] is empty, cannot read anything.")
            self._SYS_LOGGER.warning("Current partitioner [" + self.get_dir_name() +
                                     "] is empty, cannot read anything.")
            return {}

        oldest_dir = oldest_dir.replace(self._file_tree.get_root_name(), '')
        latest_dir = latest_dir.replace(self._file_tree.get_root_name(), '')

        oldest_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=oldest_dir)
        latest_timestamp = FileUtil.parse_dir_to_timestamp(dir_name=latest_dir)
        start_time = max(_reformat_time(params['start_time']), oldest_timestamp)
        end_time = min(_reformat_time(params['end_time']), latest_timestamp)
        result = {}
        try:
            all_file_names = []
            while start_time <= end_time:
                dir_list = FileUtil.parse_timestamp_to_dir(timestamp=start_time).split('/')
                dir_name = '/'.join(dir_list[:self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]])
                dir_name = FileUtil.join_paths_to_dir(
                    root_dir=self._file_tree.get_root_name(),
                    base_name=dir_name
                )
                try:
                    file_names = FileUtil.list_files_in_dir(dir_name=dir_name)
                    all_file_names.extend(file_names)
                except Exception as _:
                    pass

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
            result = {}
            if self._underlying_storage.get_storage_type() == StorageType.PROTO_TABLE_STORAGE:
                tmp_result = gclient_ext.read_proto_messages(paths=all_file_names, message_type=ProtoTable)
                for file_name, v in tmp_result.items():
                    result[file_name] = dict(v.data)
            else:
                tmp_result = gclient_ext.read_txts(all_file_names)
                for file_name, v in tmp_result.items():
                    result[file_name] = v.rstrip().split('\n')

            return result
        except Exception as err:
            self._SYS_LOGGER.error("Read range in dir [" + self.get_dir_name() + "] got exception " + str(err) + '.')
            self._logger.error("Read range in dir [" + self.get_dir_name() + "] got exception " + str(err) + '.')
            raise StorageReadException("Read range in dir [" + self.get_dir_name() + "] got exception " +
                                       str(err) + '.')

    def make_new_partition(self, timestamp):
        new_dir_list = FileUtil.parse_timestamp_to_dir(timestamp=timestamp).split('/')
        for i in range(1, self.PARTITIONER_TYPE_TO_HEIGHT_MAP[self.PARTITIONER_TYPE]):
            new_dir = '/'.join(new_dir_list[:i])
            child_node_name = FileUtil.join_paths_to_dir(
                root_dir=self._file_tree.get_root_name(),
                base_name=new_dir
            )
            if self._file_tree.find_node(child_node_name):
                parent_node_name = FileUtil.join_paths_to_dir(
                    root_dir=self._file_tree.get_root_name(),
                    base_name='/'.join(new_dir_list[:i-1]) if i > 1 else ''
                )
                parent_node = self._file_tree.find_node(node_name=parent_node_name)
                child_node = OrderedNodeBase(
                    node_name=child_node_name
                )

                assert parent_node is not None, "Parent node at least needs to exist."
                self._file_tree.add_node(
                    parent_node=parent_node,
                    child_node=child_node,
                    order=SortOrder.REVERSE
                )

        self._file_tree.trim_tree(max_capacity=self._max_capacity)

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

        if to_make_partition:
            if not params or 'timezone' not in params or params['timezone'] == 'PST':
                self.make_new_partition(timestamp=TimezoneUtil.cur_time_in_pst())
            elif params['timezone'] == 'UTC':
                self.make_new_partition(timestamp=TimezoneUtil.cur_time_in_utc())
            elif params['timezone'] == 'EST':
                self.make_new_partition(timestamp=TimezoneUtil.cur_time_in_est())

        file_name = FileUtil.join_paths_to_file(
            root_dir=self._file_tree.get_leftmost_leaf(),
            base_name=file_base_name)

        if file_name != self._underlying_storage.get_file_name():
            self._SYS_LOGGER.info("Sync to the latest file to " + file_name)
            self._underlying_storage.initialize_from_file(
                file_name=file_name
            )

        try:
            self._underlying_storage.write(data=data, params=params)
        except Exception as err:
            self._SYS_LOGGER.error("Write to dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Write to dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            raise StorageWriteException("Write to dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')

    def print_self(self):
        # for debug only
        if self._file_tree:
            self._file_tree.print_self()
