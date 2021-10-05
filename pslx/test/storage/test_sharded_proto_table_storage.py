from galaxy_py import gclient, gclient_ext
import unittest

from pslx.schema.snapshots_pb2 import NodeSnapshot
from pslx.storage.sharded_proto_table_storage import ShardedProtoTableStorage
from pslx.util.proto_util import ProtoUtil


class ShardedProtoTableStorageTest(unittest.TestCase):
    EXAMPLE_PROTO = ProtoUtil.text_to_message(
        message_type=NodeSnapshot,
        text_str="""
        node_name: "test"
        children_names: ["child_1", "child_2"]
        parents_names: ["parent_1", "parent_2"]
        """
    )
    TEST_DATA_DIR_1 = '/galaxy/ab-d/pslx/test_data/sharded_proto_table_1'
    TEST_DATA_DIR_2 = '/galaxy/ab-d/pslx/test_data/sharded_proto_table_2'
    TEST_DATA_DIR_3 = '/galaxy/ab-d/pslx/test_data/sharded_proto_table_3'

    def test_initialize_from_dir(self):
        shared_proto_table_storage = ShardedProtoTableStorage(size_per_shard=3)
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        self.assertFalse(shared_proto_table_storage.is_empty())

    def test_write_1(self):
        shared_proto_table_storage = ShardedProtoTableStorage(size_per_shard=3)
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        shared_proto_table_storage.write(
            data={
                'test_7': self.EXAMPLE_PROTO,
            }
        )
        self.assertFalse(shared_proto_table_storage.is_empty())
        self.assertEqual(shared_proto_table_storage.get_num_shards(), 3)
        gclient.rm_dir(self.TEST_DATA_DIR_1)
        gclient_ext.cp_folder(self.TEST_DATA_DIR_2, self.TEST_DATA_DIR_1)

    def test_write_2(self):
        shared_proto_table_storage = ShardedProtoTableStorage(size_per_shard=3)
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        shared_proto_table_storage.write(
            data={
                'test_7': self.EXAMPLE_PROTO,
                'test_8': self.EXAMPLE_PROTO,
                'test_9': self.EXAMPLE_PROTO,
            }
        )
        self.assertEqual(shared_proto_table_storage.get_num_entries(), 10)
        self.assertEqual(shared_proto_table_storage.get_num_shards(), 4)
        gclient.rm_dir(self.TEST_DATA_DIR_1)
        gclient_ext.cp_folder(self.TEST_DATA_DIR_2, self.TEST_DATA_DIR_1)

    def test_write_3(self):
        shared_proto_table_storage = ShardedProtoTableStorage(size_per_shard=3)
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        shared_proto_table_storage.write(
            data={
                'test_7': self.EXAMPLE_PROTO,
                'test_8': self.EXAMPLE_PROTO,
                'test_0': self.EXAMPLE_PROTO,
            }
        )
        self.assertEqual(shared_proto_table_storage.get_num_entries(), 9)
        self.assertEqual(shared_proto_table_storage.get_num_shards(), 3)
        gclient.rm_dir(self.TEST_DATA_DIR_1)
        gclient_ext.cp_folder(self.TEST_DATA_DIR_2, self.TEST_DATA_DIR_1)

    def test_read_1(self):
        shared_proto_table_storage = ShardedProtoTableStorage()
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        data = shared_proto_table_storage.read(
            params={
                'key': 'test_0'
            }
        )
        self.assertTrue(data is not None)

    def test_read_2(self):
        shared_proto_table_storage = ShardedProtoTableStorage()
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        data = shared_proto_table_storage.read_multiple(
            params={
                'keys': ['test_0', 'test_100']
            }
        )
        self.assertTrue('test_100' not in data)

    def test_read_3(self):
        shared_proto_table_storage = ShardedProtoTableStorage()
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        data = shared_proto_table_storage.read(
            params={
                'key': 'test_100'
            }
        )
        self.assertIsNone(data)

    def test_read_4(self):
        shared_proto_table_storage = ShardedProtoTableStorage()
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        data = shared_proto_table_storage.read_multiple(
            params={
                'keys': ['test_0', 'test_1']
            }
        )
        self.assertTrue('test_0' in data and 'test_1' in data)

    def test_read_5(self):
        shared_proto_table_storage = ShardedProtoTableStorage()
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        shared_proto_table_storage.write(
            data={
                'test_7': self.EXAMPLE_PROTO,
                'test_8': self.EXAMPLE_PROTO,
                'test_9': self.EXAMPLE_PROTO,
            }
        )
        data = shared_proto_table_storage.read_multiple(
            params={
                'keys': ['test_9']
            }
        )
        self.assertTrue('test_9' in data)
        gclient.rm_dir(self.TEST_DATA_DIR_1)
        gclient_ext.cp_folder(self.TEST_DATA_DIR_2, self.TEST_DATA_DIR_1)

    def test_resize_to_new_table(self):
        shared_proto_table_storage = ShardedProtoTableStorage()
        shared_proto_table_storage.initialize_from_dir(dir_name=self.TEST_DATA_DIR_1)
        new_table = shared_proto_table_storage.resize_to_new_table(
            new_size_per_shard=2,
            new_dir_name=self.TEST_DATA_DIR_3
        )
        self.assertEqual(new_table.get_num_entries(), 9)
        self.assertEqual(new_table.get_num_shards(), 5)
        gclient.rm_dir(self.TEST_DATA_DIR_3)
