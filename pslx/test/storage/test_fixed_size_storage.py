from shutil import copyfile
import unittest
from pslx.schema.enums_pb2 import WriteRuleType
from pslx.storage.fixed_size_storage import FixedSizeStorage


class DefaultStorageTest(unittest.TestCase):
    TEST_DATA_1 = "pslx/test/storage/test_data/test_default_storage_data.txt"
    TEST_DATA_2 = "pslx/test/storage/test_data/test_default_storage_data_2.txt"

    def test_read_from_end_1(self):
        fixed_size_storage = FixedSizeStorage(fixed_size=1)
        fixed_size_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        data = fixed_size_storage.read()
        self.assertListEqual(data, ['1,2,3'])

    def test_read_from_end_2(self):
        fixed_size_storage = FixedSizeStorage(fixed_size=1)
        fixed_size_storage.initialize_from_file(file_name=self.TEST_DATA_1)
        data = fixed_size_storage.read(
            params={
                'num_line': 2,
                'force_load': True,
            }
        )
        self.assertListEqual(data, ['1,2,3', '2,3,4'])

    def test_write_from_end(self):
        fixed_size_storage = FixedSizeStorage(fixed_size=1)
        fixed_size_storage.initialize_from_file(file_name=self.TEST_DATA_2)
        data = [3, 4, 5]
        fixed_size_storage.write(data=data)
        data = fixed_size_storage.read()
        copyfile(self.TEST_DATA_1, self.TEST_DATA_2)
        self.assertListEqual(data, ['1,2,3'])

    def test_write_from_beginning_1(self):
        fixed_size_storage = FixedSizeStorage(fixed_size=1)
        fixed_size_storage.initialize_from_file(file_name=self.TEST_DATA_2)
        fixed_size_storage.set_config(
            config={
                'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
            }
        )
        data = [3, 4, 5]
        fixed_size_storage.write(data=data)
        data = fixed_size_storage.read()
        copyfile(self.TEST_DATA_1, self.TEST_DATA_2)
        self.assertListEqual(data, ['3,4,5'])

    def test_write_from_beginning_2(self):
        fixed_size_storage = FixedSizeStorage(fixed_size=2)
        fixed_size_storage.initialize_from_file(file_name=self.TEST_DATA_2)
        fixed_size_storage.set_config(
            config={
                'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
            }
        )
        data = [3, 4, 5]
        fixed_size_storage.write(data=data)
        data = fixed_size_storage.read(
            params={
                'num_line': 2,
                'force_load': False,
            }
        )
        copyfile(self.TEST_DATA_1, self.TEST_DATA_2)
        self.assertListEqual(data, ['3,4,5', '1,2,3'])

    def test_write_from_beginning_3(self):
        fixed_size_storage = FixedSizeStorage(fixed_size=1)
        fixed_size_storage.initialize_from_file(file_name=self.TEST_DATA_2)
        fixed_size_storage.set_config(
            config={
                'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
            }
        )
        data = [3, 4, 5]
        fixed_size_storage.write(data=data)
        data = fixed_size_storage.read(
            params={
                'num_line': 2,
                'force_load': True,
            }
        )
        copyfile(self.TEST_DATA_1, self.TEST_DATA_2)
        self.assertListEqual(data, ['3,4,5', '1,2,3'])
