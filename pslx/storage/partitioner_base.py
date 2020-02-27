import os

from pslx.core.exception import StoragePartitionerReadException
from pslx.core.tree_base import TreeBase
from pslx.core.node_base import OrderedNodeBase
from pslx.schema.enums_pb2 import StorageType, PartitionerStorageType, SortOrder
from pslx.storage.storage_base import StorageBase
from pslx.storage.default_storage import DefaultStorage
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


class PartitionerBase(StorageBase):
    STORAGE_TYPE = StorageType.PARTITIONER_STORAGE
    PARTITIONER_TYPE = None

    def __init__(self, logger=None, max_size=-1):
        super().__init__(logger=logger)
        self._file_tree = None
        self._max_size = max_size
        self._underlying_storage = DefaultStorage(logger=logger)

    def set_underlying_storage(self, storage):
        assert storage.STORAGE_TYPE != StorageType.PARTITIONER_STORAGE
        self._underlying_storage = storage

    def initialize_from_file(self, file_name):
        self.sys_log("Initialize_from_file function is not implemented for storage type "
                     + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        pass

    def initialize_from_dir(self, dir_name):
        root_node = OrderedNodeBase(
            node_name=FileUtil.create_dir_if_not_exist(dir_name=dir_name),
            order=SortOrder.REVERSE
        )
        self._file_tree = TreeBase(root=root_node, max_dict_size=self._max_size)

        def _recursive_initialize_from_dir(node):
            node_name = node.get_node_name()
            for child_node_name in sorted(os.listdir(node_name), reverse=True):
                child_node = OrderedNodeBase(
                    node_name=FileUtil.join_paths_to_dir(root_dir=node_name, class_name=child_node_name),
                    order=SortOrder.REVERSE
                )
                if os.path.isdir(child_node.get_node_name()):
                    if self._file_tree.get_num_nodes() >= self._max_size > 0:
                        self.sys_log("Reach the max number of node: " + str(self._max_size))
                        return
                    self._file_tree.add_node(
                        parent_node=node,
                        child_node=child_node
                    )
                    _recursive_initialize_from_dir(node=child_node)
                else:
                    return

        _recursive_initialize_from_dir(node=root_node)

    def is_empty(self):
        leftmost_leaf_name = self._file_tree.get_leftmost_leaf()
        if not os.listdir(leftmost_leaf_name):
            return True
        else:
            return False

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
        if self.is_empty():
            self.sys_log("Current partitioner is empty, cannot read anything.")
            return []

        self.sys_log("Read from the latest partition.")
        latest_dir = self.get_latest_dir()
        file_names = os.listdir(latest_dir)
        if len(file_names):
            self.sys_log("Partitioner only allow one file in the patition during read.")
            raise StoragePartitionerReadException

        file_name = FileUtil.join_paths_to_file(root_dir=latest_dir, class_name=file_names[0])
        if file_name != self._underlying_storage.get_file_name():
            self._underlying_storage.initialize_from_file(file_name=file_name)

        return self._underlying_storage.read(params=params)

    def make_new_partition(self, timestamp):
        pass

    def write(self, data, params=None):
        pass
