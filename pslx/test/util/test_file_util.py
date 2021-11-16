import datetime
import unittest
from pslx.schema.enums_pb2 import ModeType
from pslx.util.file_util import FileUtil


class FileUtilTest(unittest.TestCase):

    TEST_DATA_PATH = '/galaxy/bb-d/pslx/test_data/test.json'
    TEST_DATA_PATH_1 = '/galaxy/bb-d/pslx/test_data/test.txt'

    def test_base_name(self):
        file_name = 'foo/bar/file.txt'
        self.assertEqual(FileUtil.base_name(file_name), 'file.txt')

    def test_dir_name(self):
        file_name = 'foo/bar/file.txt'
        self.assertEqual(FileUtil.dir_name(file_name), 'foo/bar')

    def test_join_paths_to_file(self):
        root_dir = 'database'
        base_name = 'foo'
        self.assertEqual(FileUtil.join_paths_to_file(root_dir, base_name), 'database/foo')

    def test_join_paths_to_dir(self):
        root_dir = 'database'
        base_name = 'foo'
        self.assertEqual(FileUtil.join_paths_to_dir(root_dir, base_name), 'database/foo/')

    def test_get_mode(self):
        file_path = 'database/TEST/ttl=1/foo'
        self.assertEqual(FileUtil.get_mode(file_path), ModeType.TEST)
        file_path = 'database/PROD/ttl=1/foo'
        self.assertEqual(FileUtil.get_mode(file_path), ModeType.PROD)

    def test_parse_timestamp_to_dir(self):
        timestamp = datetime.datetime(2020, 1, 1, 12, 30)
        self.assertEqual(
            FileUtil.parse_timestamp_to_dir(timestamp=timestamp),
            '2020/01/01/12/30'
        )
        timestamp = datetime.datetime(2020, 1, 1)
        self.assertEqual(
            FileUtil.parse_timestamp_to_dir(timestamp=timestamp),
            '2020/01/01/00/00'
        )

    def test_parse_dir_to_timestamp(self):
        dir_name = '2020/01/01/12/30'
        self.assertEqual(
            FileUtil.parse_dir_to_timestamp(dir_name=dir_name),
            datetime.datetime(2020, 1, 1, 12, 30)
        )
        dir_name = '2020/01/01/00/00'
        self.assertEqual(
            FileUtil.parse_dir_to_timestamp(dir_name=dir_name),
            datetime.datetime(2020, 1, 1)
        )
        dir_name = '2020/01'
        self.assertEqual(
            FileUtil.parse_dir_to_timestamp(dir_name=dir_name),
            datetime.datetime(2020, 1, 1)
        )

    def test_normalize_path(self):
        dir_name = 'test/foo//'
        self.assertEqual(FileUtil.normalize_dir_name(dir_name=dir_name), 'test/foo/')
        file_name = 'test/foo.txt'
        self.assertEqual(FileUtil.normalize_file_name(file_name=file_name), 'test/foo.txt')

    def test_json_to_file(self):
        data = {
            'name': "123",
            'property': 123,
        }
        FileUtil.write_json_to_file(json_obj=data, file_name=self.TEST_DATA_PATH)
        self.assertDictEqual(data, FileUtil.read_json_from_file(file_name=self.TEST_DATA_PATH))

    def test_lined_txt_to_file(self):
        data = ['123', '234']
        FileUtil.write_lined_txt_to_file(data=data, file_name=self.TEST_DATA_PATH_1)
        self.assertEqual(data, FileUtil.read_lined_txt_from_file(file_name=self.TEST_DATA_PATH_1))

    def test_get_cell_from_path(self):
        path = '/galaxy/aa-d/test'
        self.assertEqual('aa', FileUtil.get_cell_from_path(path))
        path = '/LOCAL/test'
        self.assertNotEqual('LOCAL', FileUtil.get_cell_from_path(path))
