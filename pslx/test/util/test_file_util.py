import unittest
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
        class_name = 'foo'
        ttl = 1
        self.assertTrue(FileUtil.join_paths_to_file_with_mode(root_dir, class_name, ttl) in
                        ['database/TEST/foo/ttl=1', 'database/PROD/foo/ttl=1'])

    def test_join_paths_to_dir_with_mode(self):
        root_dir = 'database'
        class_name = 'foo'
        ttl = 1
        self.assertTrue(FileUtil.join_paths_to_dir_with_mode(root_dir, class_name, ttl) in
                        ['database/TEST/foo/ttl=1/', 'database/PROD/foo/ttl=1/'])

    def test_join_paths_to_file(self):
        root_dir = 'database'
        class_name = 'foo'
        ttl = 1
        self.assertEqual(FileUtil.join_paths_to_file(root_dir, class_name, ttl), 'database/foo/ttl=1')

    def test_join_paths_to_dir(self):
        root_dir = 'database'
        class_name = 'foo'
        ttl = 1
        self.assertEqual(FileUtil.join_paths_to_dir(root_dir, class_name, ttl), 'database/foo/ttl=1/')

    def test_get_mode(self):
        file_path = 'database/TEST/foo/ttl=1'
        self.assertEqual(FileUtil.get_mode(file_path), ModeType.TEST)
        file_path = 'database/PROD/foo/ttl=1'
        self.assertEqual(FileUtil.get_mode(file_path), ModeType.PROD)
