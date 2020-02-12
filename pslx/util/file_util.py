import os
from pslx.core.exception import FileNotExistException
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
    def create_if_not_exist(cls, file_name):
        if not os.path.exists(file_name):
            dir_name = cls.dir_name(file_name=file_name)
            os.makedirs(dir_name)

        return file_name

    @classmethod
    def die_if_not_exist(cls, file_name):
        if not os.path.exists(file_name):
            raise FileNotExistException
        else:
            return file_name

    @classmethod
    def join_paths(cls, root_dir, class_name, ttl=-1):
        if 'TEST' not in os.environ or not os.environ['TEST']:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.TEST))
        else:
            pre_path = os.path.join(root_dir, ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.PROD))

        return os.path.join(pre_path, class_name, 'ttl=' + str(ttl))

    @classmethod
    def get_file_names(cls, dir_name):
        if not os.path.exists(dir_name):
            return []
        file_names = sorted([
            file_name for file_name in os.listdir(dir_name) if os.path.isfile(os.path.join(dir_name, file_name))]
        )
        return [os.path.join(dir_name, file_name) for file_name in file_names]

    @classmethod
    def get_mode(cls, file_path):
        if 'TEST' in file_path:
            return ModeType.TEST
        else:
            return ModeType.PROD

    @classmethod
    def write_proto_to_file(cls, proto, file_name):
        with open(FileUtil.create_if_not_exist(file_name=file_name), 'wb') as outfile:
            outfile.write(proto.SerializeToString())

    @classmethod
    def read_proto_from_file(cls, proto_type, file_name):
        proto = proto_type()
        try:
            with open(FileUtil.die_if_not_exist(file_name=file_name), 'rb') as infile:
                proto.ParseFromString(infile.read())
        except FileNotExistException as _:
            pass
        return proto
