import os
from pslx.core.exception import FileNotExistException
from pslx.schema.enums_pb2 import ModeType


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
            pre_path = os.path.join(root_dir, 'TEST')
        else:
            pre_path = os.path.join(root_dir, 'PROD')

        return os.path.join(pre_path, class_name, 'ttl=' + str(ttl))

    @classmethod
    def get_mode(cls, file_path):
        if 'TEST' in file_path:
            return ModeType.TEST
        else:
            return ModeType.PROD
