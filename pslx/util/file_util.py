import glob
import os
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
    def create_file_if_not_exist(cls, file_name):
        dir_name = cls.dir_name(file_name=file_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            with open(file_name, 'w') as _:
                pass

        return file_name

    @classmethod
    def create_dir_if_not_exist(cls, dir_name):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        return dir_name

    @classmethod
    def die_if_file_not_exist(cls, file_name):
        if not os.path.exists(file_name):
            raise FileNotExistException
        else:
            return file_name

    @classmethod
    def die_if_dir_not_exist(cls, dir_name):
        if not os.path.exists(dir_name):
            raise DirNotExistException
        else:
            return dir_name

    @classmethod
    def join_paths_to_file_with_mode(cls, root_dir, class_name, ttl=-1):
        if 'PSLX_TEST' not in os.environ or not os.environ['PSLX_TEST']:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.PROD))
        else:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.TEST))
        if ttl < 0:
            return os.path.join(pre_path, class_name)
        else:
            return os.path.join(pre_path, class_name, 'ttl=' + str(ttl))

    @classmethod
    def join_paths_to_dir_with_mode(cls, root_dir, class_name, ttl=-1):
        return cls.join_paths_to_file_with_mode(root_dir=root_dir, class_name=class_name, ttl=ttl) + '/'

    @classmethod
    def join_paths_to_file(cls, root_dir, class_name, ttl=-1):
        if ttl < 0:
            return os.path.join(root_dir, class_name)
        else:
            return os.path.join(root_dir, class_name, 'ttl=' + str(ttl))

    @classmethod
    def join_paths_to_dir(cls, root_dir, class_name, ttl=-1):
        return cls.join_paths_to_file(root_dir=root_dir, class_name=class_name, ttl=ttl) + '/'

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
