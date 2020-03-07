import datetime
import os
import unittest
from pslx.batch.container import DefaultBatchContainer
from pslx.schema.enums_pb2 import ModeType
from pslx.util.file_util import FileUtil


class FileUtilTest(unittest.TestCase):

    def test_base_name(self):
        file_name = 'foo/bar/file.txt'
        self.assertEqual(FileUtil.base_name(file_name), 'file.txt')

    def test_dir_name(self):
        file_name = 'foo/bar/file.txt'
        self.assertEqual(FileUtil.dir_name(file_name), 'foo/bar')

    def test_join_paths_to_file_with_mode(self):
        root_dir = 'database'
        base_name = 'foo'
        ttl = 1
        self.assertTrue(FileUtil.join_paths_to_file_with_mode(root_dir, base_name, ttl) in
                        ['database/TEST/ttl=1/foo', 'database/PROD/ttl=1/foo'])

    def test_join_paths_to_dir_with_mode(self):
        root_dir = 'database'
        base_name = 'foo'
        ttl = 1
        self.assertTrue(FileUtil.join_paths_to_dir_with_mode(root_dir, base_name, ttl) in
                        ['database/TEST/ttl=1/foo/', 'database/PROD/ttl=1/foo/'])

    def test_join_paths_to_file(self):
        root_dir = 'database'
        base_name = 'foo'
        ttl = 1
        self.assertEqual(FileUtil.join_paths_to_file(root_dir, base_name, ttl), 'database/ttl=1/foo')

    def test_join_paths_to_dir(self):
        root_dir = 'database'
        base_name = 'foo'
        ttl = 1
        self.assertEqual(FileUtil.join_paths_to_dir(root_dir, base_name, ttl), 'database/ttl=1/foo/')

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

    def test_create_container_snapshot_pattern(self):
        container_name = "container_foo"
        container_class = DefaultBatchContainer
        database = os.getenv('DATABASE', 'database/')
        self.assertTrue(FileUtil.create_container_snapshot_pattern(
            container_name=container_name,
            contain_class=container_class
        ).replace(database, '') in ["PROD/DefaultBatchContainer__container_foo/SNAPSHOT_*_container_foo.pb",
                                    "TEST/DefaultBatchContainer__container_foo/SNAPSHOT_*_container_foo.pb"])
        self.assertTrue(FileUtil.create_container_snapshot_pattern(
            container_name=container_name,
            contain_class=container_class,
            container_ttl=1
        ).replace(database, '') in [
            "PROD/ttl=1/DefaultBatchContainer__container_foo/SNAPSHOT_*_container_foo.pb",
            "TEST/ttl=1/DefaultBatchContainer__container_foo/SNAPSHOT_*_container_foo.pb"])

    def test_create_operator_snapshot_pattern(self):
        container_name = "container_foo"
        container_class = DefaultBatchContainer
        operator_name = "operator_bar"
        database = os.getenv('DATABASE', 'database/')
        self.assertTrue(FileUtil.create_operator_snapshot_pattern(
            container_name=container_name,
            operator_name=operator_name,
            contain_class=container_class
        ).replace(database, '') in [
            "PROD/DefaultBatchContainer__container_foo/operators/SNAPSHOT_*_operator_bar.pb",
            "TEST/DefaultBatchContainer__container_foo/operators/SNAPSHOT_*_operator_bar.pb"])
        self.assertTrue(FileUtil.create_operator_snapshot_pattern(
            container_name=container_name,
            operator_name=operator_name,
            contain_class=container_class,
            container_ttl=1
        ).replace(database, '') in [
            "PROD/ttl=1/DefaultBatchContainer__container_foo/operators/SNAPSHOT_*_operator_bar.pb",
            "TEST/ttl=1/DefaultBatchContainer__container_foo/operators/SNAPSHOT_*_operator_bar.pb"])

    def test_get_file_names_from_pattern(self):
        pattern = "pslx/test/util/test_file*.py"
        result = FileUtil.get_file_names_from_pattern(pattern=pattern)
        self.assertTrue("pslx/test/util/test_file_util.py" in result)

    def test_list_dir(self):
        dir_name = "pslx/test/util/"
        self.assertTrue("pslx/test/util/test_file_util.py" in FileUtil._list_dir(dir_name=dir_name))

    def test_list_files_in_dir(self):
        dir_name = "pslx/test/util/"
        self.assertTrue("pslx/test/util/test_file_util.py" in FileUtil.list_files_in_dir(dir_name=dir_name))

    def test_list_dirs_in_dir(self):
        dir_name = "pslx/test/"
        self.assertTrue("pslx/test/util/" in FileUtil.list_dirs_in_dir(dir_name=dir_name))

    def test_get_ttl_from_path(self):
        path = "pslx/test/util/"
        self.assertTrue(FileUtil.get_ttl_from_path(path=path) is None)
        path = "pslx/ttl=-1/test/util/"
        self.assertTrue(FileUtil.get_ttl_from_path(path=path) is None)
        path = "pslx/ttl=1/test/util/"
        self.assertEqual(FileUtil.get_ttl_from_path(path=path), datetime.timedelta(days=1))
        path = "pslx/ttl=1d/test/util/"
        self.assertEqual(FileUtil.get_ttl_from_path(path=path), datetime.timedelta(days=1))
        path = "pslx/ttl=1h/test/util/"
        self.assertEqual(FileUtil.get_ttl_from_path(path=path), datetime.timedelta(hours=1))
