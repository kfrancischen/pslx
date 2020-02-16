import unittest
from pslx.schema.enums_pb2 import ModeType
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
