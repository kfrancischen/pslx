import datetime
import glob
import os
import shutil
from pslx.core.exception import FileNotExistException, DirNotExistException
from pslx.schema.enums_pb2 import ModeType
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
        return os.path.exists(file_name)

    @classmethod
    def does_dir_exist(cls, dir_name):
        return os.path.exists(dir_name)

    @classmethod
    def is_file_empty(cls, file_name):
        cls.die_if_file_not_exist(file_name=file_name)
        return os.stat(file_name).st_size == 0

    @classmethod
    def is_dir_empty(cls, dir_name):
        cls.die_if_dir_not_exist(dir_name=dir_name)
        return len(os.listdir(dir_name)) == 0

    @classmethod
    def create_file_if_not_exist(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        cls.create_dir_if_not_exist(dir_name=cls.dir_name(file_name=file_name))
        if not cls.does_file_exist(file_name=file_name):
            with open(file_name, 'w') as _:
                pass

        return file_name

    @classmethod
    def create_dir_if_not_exist(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        if not cls.does_dir_exist(dir_name=dir_name):
            os.makedirs(dir_name)
        return dir_name

    @classmethod
    def die_if_file_not_exist(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        if not cls.does_file_exist(file_name=file_name):
            raise FileNotExistException
        else:
            return file_name

    @classmethod
    def die_if_dir_not_exist(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        if not cls.does_dir_exist(dir_name=dir_name):
            raise DirNotExistException
        else:
            return dir_name

    @classmethod
    def _list_dir(cls, dir_name):
        cls.normalize_dir_name(dir_name=dir_name)
        cls.die_if_dir_not_exist(dir_name=dir_name)
        items = os.listdir(dir_name)
        return [cls.join_paths_to_file(dir_name, item) for item in items]

    @classmethod
    def normalize_file_name(cls, file_name):
        return os.path.normpath(path=file_name)

    @classmethod
    def normalize_dir_name(cls, dir_name):
        return os.path.normpath(path=dir_name) + '/'

    @classmethod
    def list_files_in_dir(cls, dir_name):
        everything = cls._list_dir(dir_name=dir_name)
        return [cls.normalize_file_name(item) for item in everything if os.path.isfile(item)]

    @classmethod
    def list_dirs_in_dir(cls, dir_name):
        everything = cls._list_dir(dir_name=dir_name)
        return [cls.normalize_dir_name(item) for item in everything if os.path.isdir(item)]

    @classmethod
    def list_files_in_dir_recursively(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        result = []
        if not cls.does_dir_exist(dir_name=dir_name):
            return result
        for sub_dir, _, file_names in os.walk(dir_name):
            for file_name in file_names:
                result.append(cls.join_paths_to_file(root_dir=sub_dir, base_name=file_name))

        return result

    @classmethod
    def list_dirs_in_dir_recursively(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        result = []
        if not cls.does_dir_exist(dir_name=dir_name):
            return result
        for sub_dir, _, file_names in os.walk(dir_name):
            result.append(sub_dir)

        return result

    @classmethod
    def remove_file(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        if cls.does_file_exist(file_name=file_name):
            os.remove(file_name)

    @classmethod
    def remove_dir(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        if cls.does_dir_exist(dir_name=dir_name):
            shutil.rmtree(dir_name)

    @classmethod
    def get_file_modified_time(cls, file_name):
        if not cls.does_file_exist(file_name=file_name):
            return None
        mod_time = os.path.getmtime(file_name)
        return datetime.datetime.fromtimestamp(mod_time)

    @classmethod
    def get_ttl_from_path(cls, path):
        for item in path.split('/'):
            if 'ttl=' in item:
                item = item.replace('ttl=', '').lower()
                if 'd' == item[-1]:
                    return datetime.timedelta(days=int(item.replace('d', '')))
                elif 'h' == item[-1]:
                    return datetime.timedelta(hours=int(item.replace('h', '')))
                elif 'm' == item[-1]:
                    return datetime.timedelta(minutes=int(item.replace('m', '')))
                elif int(item) > 0:
                    return datetime.timedelta(days=int(item))
        return None

    @classmethod
    def join_paths_to_file_with_mode(cls, root_dir, base_name, ttl=-1):
        if 'PSLX_TEST' not in os.environ or not os.environ['PSLX_TEST']:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.PROD))
        else:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.TEST))
        if ttl < 0:
            return os.path.join(pre_path, base_name)
        else:
            return os.path.join(pre_path, 'ttl=' + str(ttl), base_name)

    @classmethod
    def join_paths_to_dir_with_mode(cls, root_dir, base_name, ttl=-1):
        return cls.join_paths_to_file_with_mode(root_dir=root_dir, base_name=base_name, ttl=ttl) + '/'

    @classmethod
    def join_paths_to_file(cls, root_dir, base_name, ttl=-1):
        if ttl < 0:
            return os.path.join(root_dir, base_name)
        else:
            return os.path.join(root_dir, 'ttl=' + str(ttl), base_name)

    @classmethod
    def join_paths_to_dir(cls, root_dir, base_name, ttl=-1):
        return cls.join_paths_to_file(root_dir=root_dir, base_name=base_name, ttl=ttl) + '/'

    @classmethod
    def get_file_names_in_dir(cls, dir_name):
        if not os.path.exists(dir_name):
            return []
        file_names = sorted([
            file_name for file_name in os.listdir(dir_name) if os.path.isfile(os.path.join(dir_name, file_name))]
        )
        return [os.path.join(dir_name, file_name) for file_name in file_names]

    @classmethod
    def get_file_names_from_pattern(cls, pattern):
        return sorted(glob.glob(pattern))

    @classmethod
    def get_mode(cls, file_path):
        if 'TEST' in file_path:
            return ModeType.TEST
        else:
            return ModeType.PROD

    @classmethod
    def write_proto_to_file(cls, proto, file_name):
        with open(FileUtil.create_file_if_not_exist(file_name=file_name), 'wb') as outfile:
            outfile.write(proto.SerializeToString())

    @classmethod
    def read_proto_from_file(cls, proto_type, file_name):
        proto = proto_type()
        try:
            with open(FileUtil.die_if_file_not_exist(file_name=file_name), 'rb') as infile:
                proto.ParseFromString(infile.read())
        except FileNotExistException as _:
            pass
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
    def create_container_snapshot_pattern(cls, container_name, contain_class, container_ttl=-1):
        contain_snapshot_dir = cls.join_paths_to_dir_with_mode(
            root_dir=os.getenv('DATABASE', 'database/'),
            base_name=contain_class.get_class_name() + '__' + container_name,
            ttl=container_ttl
        )
        return FileUtil.join_paths_to_file(
            root_dir=FileUtil.dir_name(contain_snapshot_dir),
            base_name='SNAPSHOT_*_' + container_name + '.pb'
        )

    @classmethod
    def create_operator_snapshot_pattern(cls, container_name, operator_name, contain_class, container_ttl=-1):
        contain_snapshot_dir = cls.join_paths_to_dir_with_mode(
            root_dir=os.getenv('DATABASE', 'database/'),
            base_name=contain_class.get_class_name() + '__' + container_name,
            ttl=container_ttl
        )
        return FileUtil.join_paths_to_file(
            root_dir=FileUtil.join_paths_to_dir(FileUtil.dir_name(contain_snapshot_dir), 'operators'),
            base_name='SNAPSHOT_*_' + operator_name + '.pb'
        )
