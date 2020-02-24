from shutil import copyfile
import unittest
from pslx.schema.enums_pb2 import ReadRuleType, WriteRuleType
from pslx.storage.default_storage import DefaultStorage


class DefaultStorageTest(unittest.TestCase):
    TEST_DATA_1 = "pslx/test/storage/test_data/test_default_storage_data.txt"
    TEST_DATA_2 = "pslx/test/storage/test_data/test_default_storage_data_2.txt"

    def test_initialize_from_file(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        self.assertEqual(default_storage.get_file_name(), self.TEST_DATA_1)

    def test_read_from_beginning_1(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        data = default_storage.read()
        self.assertEqual(data, ['1,2,3'])

    def test_read_from_beginning_2(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        data = default_storage.read({
            'num_line': 2,
        })
        self.assertEqual(data, ['1,2,3', '2,3,4'])

    def test_read_from_end_1(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        default_storage.set_config(
            config={
                'read_rule_type': ReadRuleType.READ_FROM_END,
            }
        )
        data = default_storage.read()
        self.assertEqual(data, ['2,3,4'])

    def test_read_from_end_2(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        default_storage.set_config(
            config={
                'read_rule_type': ReadRuleType.READ_FROM_END,
            }
        )
        data = default_storage.read({
            'num_line': 2,
        })
        self.assertEqual(data, ['2,3,4', '1,2,3'])

    def test_write_from_end(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_2)
        data = [3, 4, 5]
        default_storage.set_config(
            config={
                'read_rule_type': ReadRuleType.READ_FROM_END,
            }
        )
        default_storage.write(data=data)
        data = default_storage.read()
        self.assertEqual(data, ['3,4,5'])
        copyfile(self.TEST_DATA_1, self.TEST_DATA_2)

    def test_write_from_beginning(self):
        default_storage = DefaultStorage()
        default_storage.initialize_from_file(file_name=self.TEST_DATA_2)
        data = [3, 4, 5]
        default_storage.set_config(
            config={
                'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
            }
        )
        default_storage.write(data=data)
        data = default_storage.read()
        self.assertEqual(data, ['3,4,5'])
        copyfile(self.TEST_DATA_1, self.TEST_DATA_2)
