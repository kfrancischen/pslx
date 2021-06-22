import datetime
from galaxy_py import gclient, gclient_ext
import unittest
from pslx.schema.enums_pb2 import WriteRuleType
from pslx.schema.snapshots_pb2 import NodeSnapshot
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.storage.partitioner_storage import YearlyPartitionerStorage, MonthlyPartitionerStorage
from pslx.util.proto_util import ProtoUtil


class PartitionerStorageTest(unittest.TestCase):
    YEARLY_PATITIONER_TEST_DATA = "/galaxy/bb-d/pslx/test_data/yearly_partitioner_1/"
    YEARLY_PATITIONER_TEST_DATA_2 = "/galaxy/bb-d/pslx/test_data/yearly_partitioner_2/"
    YEARLY_PATITIONER_TEST_DATA_3 = "/galaxy/bb-d/pslx/test_data/yearly_partitioner_3/"
    YEARLY_PATITIONER_TEST_DATA_4 = "/galaxy/bb-d/pslx/test_data/yearly_partitioner_4/"
    MONTHLY_PATITIONER_TEST_DATA = "/galaxy/bb-d/pslx/test_data/monthly_partitioner_1/"
    MONTHLY_PATITIONER_TEST_DATA_2 = "/galaxy/bb-d/pslx/test_data/monthly_partitioner_2/"

    def test_initialize_from_dir(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA)
        self.assertFalse(partitioner.is_empty())
        self.assertEqual(partitioner.get_latest_dir(), self.YEARLY_PATITIONER_TEST_DATA + '2020/')

    def test_read_1(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA)
        data = partitioner.read(
            params={
                'num_line': 2,
            }
        )
        self.assertListEqual(data, ['1,2,3', '2,3,4'])

    def test_read_2(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA_4)
        proto_table_storage = ProtoTableStorage()
        partitioner.set_underlying_storage(storage=proto_table_storage)
        data = partitioner.read(
            params={
                'key': 'test',
            }
        )
        proto_text_str = """
        node_name: "test"
        children_names: ["child_1", "child_2"]
        parents_names: ["parent_1", "parent_2"]
        """
        val = ProtoUtil.message_to_any(
                message=ProtoUtil.text_to_message(
                    message_type=NodeSnapshot,
                    text_str=proto_text_str
                )
            )
        self.assertEqual(data, val)
        data = partitioner.read(
            params={
                'key': 'test1',
            }
        )
        self.assertIsNone(data)

    def test_get_oldest_dir_in_root_directory(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA_4)
        proto_table_storage = ProtoTableStorage()
        partitioner.set_underlying_storage(storage=proto_table_storage)
        self.assertEqual(partitioner.get_oldest_dir_in_root_directory(),
                         '/galaxy/bb-d/pslx/test_data/yearly_partitioner_4/2019/')

    def test_read_range_1(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA_3)
        start_time = datetime.datetime(2019, 1, 5)
        end_time = datetime.datetime(2020, 1, 5)
        data = partitioner.read_range(params={
            'start_time': start_time,
            'end_time': end_time,
        })
        self.assertDictEqual(data, {self.YEARLY_PATITIONER_TEST_DATA_3 + '2019/data': ['1,2,3', '2,3,4'],
                                    self.YEARLY_PATITIONER_TEST_DATA_3 + '2020/data': ['1,2,3', '2,3,4']})

    def test_read_range_2(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA_4)
        proto_table_storage = ProtoTableStorage()
        partitioner.set_underlying_storage(storage=proto_table_storage)
        start_time = datetime.datetime(2019, 1, 5)
        end_time = datetime.datetime(2020, 1, 5)
        data = partitioner.read_range(params={
            'start_time': start_time,
            'end_time': end_time,
        })

        proto_text_str = """
        node_name: "test"
        children_names: ["child_1", "child_2"]
        parents_names: ["parent_1", "parent_2"]
        """
        val = ProtoUtil.message_to_any(
                message=ProtoUtil.text_to_message(
                    message_type=NodeSnapshot,
                    text_str=proto_text_str
                )
            )

        self.assertDictEqual(data, {
            self.YEARLY_PATITIONER_TEST_DATA_4 + '2019/data.pb': {'test': val},
            self.YEARLY_PATITIONER_TEST_DATA_4 + '2020/data.pb': {'test': val},
        })

    def test_write_1(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA)
        data = [3, 4, 5]
        partitioner.write(data=data, params={'make_partition': False})
        self.assertListEqual(partitioner.read(), ["1,2,3"])
        partitioner.read()
        self.assertListEqual(partitioner.read(), ["3,4,5"])
        self.assertEqual(partitioner.get_size(), 2)
        gclient.rm_dir_recursive(self.YEARLY_PATITIONER_TEST_DATA)
        gclient_ext.cp_folder(self.YEARLY_PATITIONER_TEST_DATA_2, self.YEARLY_PATITIONER_TEST_DATA)

    def test_write_2(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA)
        partitioner.set_config(
            config={
                'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
            }
        )
        partitioner.write(data=[3, 4, 5])
        data = partitioner.read()
        gclient.rm_dir_recursive(self.YEARLY_PATITIONER_TEST_DATA)
        gclient_ext.cp_folder(self.YEARLY_PATITIONER_TEST_DATA_2, self.YEARLY_PATITIONER_TEST_DATA)
        self.assertListEqual(data, ['3,4,5'])

    def test_write_3(self):
        partitioner = YearlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.YEARLY_PATITIONER_TEST_DATA)
        partitioner.set_config(
            config={
                'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
            }
        )
        partitioner.write(data=[3, 4, 5], params={'make_partition': False})
        data = partitioner.read(params={'num_line': 2})
        self.assertListEqual(data, ['3,4,5', '1,2,3'])
        gclient.rm_dir_recursive(self.YEARLY_PATITIONER_TEST_DATA)
        gclient_ext.cp_folder(self.YEARLY_PATITIONER_TEST_DATA_2, self.YEARLY_PATITIONER_TEST_DATA)

    def test_incremental_build(self):
        partitioner = MonthlyPartitionerStorage(max_capacity=4)
        partitioner.initialize_from_dir(dir_name=self.MONTHLY_PATITIONER_TEST_DATA)
        self.assertEqual(partitioner.get_size(), 4)
        gclient.rm_dir_recursive(self.MONTHLY_PATITIONER_TEST_DATA)
        gclient_ext.cp_folder(self.MONTHLY_PATITIONER_TEST_DATA_2, self.MONTHLY_PATITIONER_TEST_DATA)

    def test_get_previous_dir(self):
        partitioner = MonthlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.MONTHLY_PATITIONER_TEST_DATA)
        self.assertEqual(partitioner.get_previous_dir(cur_dir=partitioner.get_latest_dir()),
                         self.MONTHLY_PATITIONER_TEST_DATA + '2020/01/')

    def test_get_next_dir(self):
        partitioner = MonthlyPartitionerStorage()
        partitioner.initialize_from_dir(dir_name=self.MONTHLY_PATITIONER_TEST_DATA)
        self.assertEqual(partitioner.get_next_dir(cur_dir=self.MONTHLY_PATITIONER_TEST_DATA + '2020/01/'),
                         self.MONTHLY_PATITIONER_TEST_DATA + '2020/02/')
