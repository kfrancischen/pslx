from shutil import copyfile
import unittest

from pslx.schema.snapshots_pb2 import NodeSnapshot, OperatorSnapshot
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.util.proto_util import ProtoUtil


class ProtoTableStorageTest(unittest.TestCase):
    EXAMPLE_PROTO_1 = ProtoUtil.text_to_message(
        message_type=NodeSnapshot,
        text_str="""
        node_name: "test"
        children_names: ["child_1", "child_2"]
        parents_names: ["parent_1", "parent_2"]
        """
    )
    EXAMPLE_PROTO_2 = ProtoUtil.text_to_message(
        message_type=OperatorSnapshot,
        text_str="""
        operator_name: "test"
        data_model: BATCH
        status: SUCCEEDED
        node_snapshot: {
            node_name: "test"
            children_names: ["child_1", "child_2"]
            parents_names: ["parent_1", "parent_2"]
        }
        """
    )
    EXAMPLE_PROTO_3 = ProtoUtil.text_to_message(
        message_type=OperatorSnapshot,
        text_str="""
        operator_name: "test_1"
        data_model: STREAMING
        status: RUNNING
        node_snapshot: {
            node_name: "test"
            children_names: ["child_1", "child_2"]
            parents_names: ["parent_1", "parent_2"]
        }
        """
    )
    TEST_DATA_1 = "pslx/test/storage/test_data/test_proto_table_data.pb"
    TEST_DATA_2 = "pslx/test/storage/test_data/test_proto_table_data_2.pb"
    TEST_DATA_3 = "pslx/test/storage/test_data/test_proto_table_data_3.pb"
    TEST_DATA_4 = "pslx/test/storage/test_data/test_proto_table_data_4.pb"

    def test_write_1(self):
        proto_table_storage = ProtoTableStorage()
        proto_table_storage.initialize_from_file(
            file_name=self.TEST_DATA_3
        )
        proto_table_storage.write(
            data=['test', self.EXAMPLE_PROTO_1],
        )
        result_proto = proto_table_storage.read(
            params={
                'key': 'test',
                'message_type': NodeSnapshot
            }
        )
        self.assertEqual(result_proto, self.EXAMPLE_PROTO_1)
        copyfile(self.TEST_DATA_1, self.TEST_DATA_3)

    def test_write_2(self):
        proto_table_storage = ProtoTableStorage()
        proto_table_storage.initialize_from_file(
            file_name=self.TEST_DATA_4
        )
        proto_table_storage.write(
            data=['test', self.EXAMPLE_PROTO_1],
        )
        proto_table_storage.write(
            data=['test_1', self.EXAMPLE_PROTO_2]
        )
        result_proto = proto_table_storage.read(
            params={
                'key': 'test_1',
                'message_type': OperatorSnapshot
            }
        )
        self.assertEqual(result_proto, self.EXAMPLE_PROTO_2)

        proto_table_storage.write(
            data=['test_1', self.EXAMPLE_PROTO_3]
        )
        result_proto = proto_table_storage.read(
            params={
                'key': 'test_1',
                'message_type': OperatorSnapshot
            }
        )
        self.assertEqual(result_proto, self.EXAMPLE_PROTO_3)
        self.assertEqual(proto_table_storage.get_num_entries(), 2)
        copyfile(self.TEST_DATA_2, self.TEST_DATA_4)

    def test_write_3(self):
        proto_table_storage = ProtoTableStorage()
        proto_table_storage.initialize_from_file(
            file_name=self.TEST_DATA_4
        )
        proto_table_storage.write(
            data=['test', self.EXAMPLE_PROTO_1],
        )
        proto_table_storage.write(
            data=['test_1', self.EXAMPLE_PROTO_2]
        )
        result_proto = proto_table_storage.read(
            params={
                'key': 'test_1',
                'message_type': OperatorSnapshot
            }
        )
        self.assertEqual(result_proto, self.EXAMPLE_PROTO_2)

        proto_table_storage.write(
            data=['test_1', self.EXAMPLE_PROTO_3],
            params={
                'overwrite': False,
            }
        )
        result_proto = proto_table_storage.read(
            params={
                'key': 'test_1',
                'message_type': OperatorSnapshot
            }
        )
        self.assertEqual(result_proto, self.EXAMPLE_PROTO_2)
        self.assertEqual(proto_table_storage.get_num_entries(), 2)
        copyfile(self.TEST_DATA_2, self.TEST_DATA_4)

