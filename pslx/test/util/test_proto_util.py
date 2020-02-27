import unittest
from pslx.schema.enums_pb2 import ModeType
from pslx.schema.snapshots_pb2 import NodeSnapshot
from pslx.util.proto_util import ProtoUtil


class ProtoUtilTest(unittest.TestCase):

    def test_enum_valid(self):
        test_enum_type = ModeType
        test_value = ModeType.TEST
        self.assertTrue(ProtoUtil.check_valid_enum(enum_type=test_enum_type, value=test_value))

    def test_get_name_by_value(self):
        test_enum_type = ModeType
        test_value = ModeType.TEST
        self.assertEqual(ProtoUtil.get_name_by_value(enum_type=test_enum_type, value=test_value), 'TEST')

    def test_get_value_by_name(self):
        test_enum_type = ModeType
        test_name = 'TEST'
        self.assertEqual(ProtoUtil.get_value_by_name(enum_type=test_enum_type, name=test_name), ModeType.TEST)

    def test_message_to_any(self):
        node_snapshot = NodeSnapshot()
        node_snapshot.node_name = 'test'
        node_snapshot.children_names.append('children1')
        node_snapshot.children_names.append('children2')
        node_snapshot.parents_names.append('parent1')
        node_snapshot.parents_names.append('parent2')

        any_message = ProtoUtil.message_to_any(message=node_snapshot)
        self.assertEqual(ProtoUtil.any_to_message(message_type=NodeSnapshot, any_message=any_message), node_snapshot)
