import datetime
import glob
import os
from galaxy_py import gclient, gclient_ext
from pslx.core.exception import FileNotExistException, DirNotExistException
from pslx.schema.enums_pb2 import ModeType
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil


class FileUtil(object):
    @classmethod
    def base_name(cls, file_name):
        return os.path.basename(file_name)

    @classmethod
    def dir_name(cls, file_name):
        return os.path.dirname(file_name)

    @classmethod
    def does_file_exist(cls, file_name):
        file_name = gclient.file_or_die(path=file_name)
        if file_name:
            return True
        else:
            return False

    @classmethod
    def does_dir_exist(cls, dir_name):
        dir_name = gclient.dir_or_die(path=dir_name)
        if dir_name:
            return True
        else:
            return False

    @classmethod
    def is_file_empty(cls, file_name):
        cls.die_if_file_not_exist(file_name=file_name)
        return gclient.read(path=file_name) == ""

    @classmethod
    def is_dir_empty(cls, dir_name):
        cls.die_if_dir_not_exist(dir_name=dir_name)
        return len(gclient_ext.list_all_in_dir(dir_name)) == 0

    @classmethod
    def create_file_if_not_exist(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        return gclient.create_file_if_not_exist(path=file_name)

    @classmethod
    def create_dir_if_not_exist(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        return gclient.create_dir_if_not_exist(path=dir_name)

    @classmethod
    def die_if_file_not_exist(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        if not cls.does_file_exist(file_name=file_name):
            raise FileNotExistException(file_name + " does not exist.")
        else:
            return file_name

    @classmethod
    def die_if_dir_not_exist(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        if not cls.does_dir_exist(dir_name=dir_name):
            raise DirNotExistException(dir_name + " does not exist.")
        else:
            return dir_name

    @classmethod
    def is_file(cls, path_name):
        return os.path.isfile(path_name)

    @classmethod
    def is_dir(cls, path_name):
        return os.path.isdir(path_name)

    @classmethod
    def normalize_file_name(cls, file_name):
        return os.path.normpath(path=file_name)

    @classmethod
    def normalize_dir_name(cls, dir_name):
        return os.path.normpath(path=dir_name) + '/'

    @classmethod
    def list_files_in_dir(cls, dir_name):
        return list(gclient.list_files_in_dir(path=dir_name).keys())

    @classmethod
    def list_dirs_in_dir(cls, dir_name):
        return list(gclient.list_dirs_in_dir(path=dir_name).keys())

    @classmethod
    def list_files_in_dir_recursively(cls, dir_name):
        return list(gclient.list_files_in_dir_recursive(path=dir_name).keys())

    @classmethod
    def list_dirs_in_dir_recursively(cls, dir_name):
        return list(gclient.list_dirs_in_dir_recursive(path=dir_name).keys())

    @classmethod
    def remove_file(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        gclient.rm_file(path=file_name)

    @classmethod
    def remove_dir_recursively(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        gclient.rm_dir_recursive(path=dir_name)

    @classmethod
    def join_paths_to_file_with_mode(cls, root_dir, base_name, ttl=-1):
        if 'PSLX_TEST' not in os.environ or not os.environ['PSLX_TEST']:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.PROD))
        else:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.TEST))
        if isinstance(ttl, int) and ttl < 0:
            return os.path.join(pre_path, base_name)
        else:
            return os.path.join(pre_path, 'ttl=' + str(ttl), base_name)

    @classmethod
    def join_paths_to_dir_with_mode(cls, root_dir, base_name, ttl=-1):
        return cls.join_paths_to_file_with_mode(root_dir=root_dir, base_name=base_name, ttl=ttl) + '/'

    @classmethod
    def join_paths_to_file(cls, root_dir, base_name, ttl=-1):
        if isinstance(ttl, int) and ttl < 0:
            return os.path.join(root_dir, base_name)
        else:
            return os.path.join(root_dir, 'ttl=' + str(ttl), base_name)

    @classmethod
    def join_paths_to_dir(cls, root_dir, base_name, ttl=-1):
        path = cls.join_paths_to_file(root_dir=root_dir, base_name=base_name, ttl=ttl)
        if path[-1] != '/':
            path += '/'
        return path

    @classmethod
    def get_file_names_from_pattern(cls, pattern):
        return sorted(glob.glob(pattern))

    @classmethod
    def get_mode(cls, path_name):
        if 'TEST' in path_name:
            return ModeType.TEST
        else:
            return ModeType.PROD

    @classmethod
    def write_proto_to_file(cls, proto, file_name):
        gclient_ext.write_proto_message(path=file_name, data=proto)

    @classmethod
    def read_proto_from_file(cls, proto_type, file_name):
        proto = gclient_ext.read_proto_message(path=file_name, message_type=proto_type)
        return proto

    @classmethod
    def parse_timestamp_to_dir(cls, timestamp):
        timestamp_list = [str(timestamp.year), '%02d' % timestamp.month, '%02d' % timestamp.day,
                          '%02d' % timestamp.hour, '%02d' % timestamp.minute]
        return '/'.join(timestamp_list)

    @classmethod
    def parse_dir_to_timestamp(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        dir_name_list = dir_name.split('/')[:-1]
        dir_name_list = [int(val) for val in dir_name_list]
        for _ in range(len(dir_name_list), 3):
            dir_name_list.append(1)
        for _ in range(len(dir_name_list), 5):
            dir_name_list.append(0)
        return datetime.datetime(dir_name_list[0], dir_name_list[1], dir_name_list[2], dir_name_list[3],
                                 dir_name_list[4])

    @classmethod
    def create_container_snapshot_pattern(cls, container_name, container_class, container_ttl=-1):
        contain_snapshot_dir = cls.join_paths_to_dir_with_mode(
            root_dir=EnvUtil.get_pslx_env_variable('PSLX_DATABASE'),
            base_name=container_class.get_class_name() + '__' + container_name,
            ttl=container_ttl
        )
        return FileUtil.join_paths_to_file(
            root_dir=FileUtil.dir_name(contain_snapshot_dir),
            base_name='SNAPSHOT_*_' + container_name + '.pb'
        )

    @classmethod
    def create_operator_snapshot_pattern(cls, container_name, operator_name, container_class, container_ttl=-1):
        contain_snapshot_dir = cls.join_paths_to_dir_with_mode(
            root_dir=EnvUtil.get_pslx_env_variable('PSLX_DATABASE'),
            base_name=container_class.get_class_name() + '__' + container_name,
            ttl=container_ttl
        )
        return FileUtil.join_paths_to_file(
            root_dir=FileUtil.join_paths_to_dir(FileUtil.dir_name(contain_snapshot_dir), 'operators'),
            base_name='SNAPSHOT_*_' + operator_name + '.pb'
        )
