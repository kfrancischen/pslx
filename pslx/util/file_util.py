import datetime
import glob
import json
import os
from galaxy_py import gclient, gclient_ext
from pslx.core.exception import FileNotExistException, DirNotExistException
from pslx.schema.enums_pb2 import ModeType
from pslx.util.env_util import EnvUtil


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
        try:
            attr = json.loads(gclient.get_attr(path=file_name))
            if 'size' in attr:
                return int(attr['size']) == 0
        except Exception as _:
            return True

    @classmethod
    def is_dir_empty(cls, dir_name):
        try:
            return len(gclient_ext.list_all_in_dir(dir_name)) == 0
        except Exception as _:
            return True

    @classmethod
    def create_file_if_not_exist(cls, file_name):
        file_name = cls.normalize_file_name(file_name=file_name)
        gclient.create_file_if_not_exist(path=file_name)
        return file_name

    @classmethod
    def create_dir_if_not_exist(cls, dir_name):
        dir_name = cls.normalize_dir_name(dir_name=dir_name)
        gclient.create_dir_if_not_exist(path=dir_name)
        return dir_name

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
    def copy_file(cls, from_path, to_path):
        gclient.copy_file(from_path, to_path)

    @classmethod
    def move_file(cls, from_path, to_path):
        gclient.move_file(from_path, to_path)

    @classmethod
    def copy_folder(cls, from_path, to_path):
        gclient_ext.copy_folder(from_path, to_path)

    @classmethod
    def move_folder(cls, from_path, to_path):
        gclient_ext.move_folder(from_path, to_path)

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
        return [dir + '/' for dir in gclient.list_dirs_in_dir(path=dir_name).keys()]

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
    def join_paths_to_file(cls, root_dir, base_name):
        return os.path.join(root_dir, base_name)

    @classmethod
    def join_paths_to_dir(cls, root_dir, base_name):
        path = cls.join_paths_to_file(root_dir=root_dir, base_name=base_name)
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
    def read(cls, file_name):
        return gclient_ext.read_txt(file_name)

    @classmethod
    def write(cls, file_name, data, mode='w'):
        return gclient.write(file_name, data, mode)

    @classmethod
    def write_proto_to_file(cls, proto, file_name):
        gclient_ext.write_proto_message(path=file_name, data=proto)

    @classmethod
    def read_proto_from_file(cls, proto_type, file_name):
        proto = gclient_ext.read_proto_message(path=file_name, message_type=proto_type)
        return proto

    @classmethod
    def write_json_to_file(cls, json_obj, file_name):
        data = json.dumps(json_obj, indent=2)
        gclient.write(path=file_name, data=data)

    @classmethod
    def read_json_from_file(cls, file_name):
        data = gclient_ext.read_txt(path=file_name)
        if data:
            return json.loads(data)
        else:
            return {}

    @classmethod
    def read_lined_txt_from_file(cls, file_name):
        data = gclient_ext.read_txt(file_name)
        return [item.strip() for item in data.rstrip().split('\n')]

    @classmethod
    def write_lined_txt_to_file(cls, data, file_name):
        gclient.write(path=file_name, data='\n'.join(data))

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
    def is_local_path(cls, path):
        if not path or '/LOCAL' in path:
            return True
        else:
            return False

    @classmethod
    def convert_local_to_cell_path(cls, path, cell=''):
        if not path:
            return ''
        if cls.is_local_path(path) and EnvUtil.get_other_env_variable(var='GALAXY_fs_cell'):
            cell_name = cell if cell else EnvUtil.get_other_env_variable(var='GALAXY_fs_cell')
            path = path.replace('/LOCAL', '/galaxy/' + cell_name + '-d')

        return path

    @classmethod
    def get_cell_from_path(cls, path):
        if cls.is_local_path(path):
            return EnvUtil.get_other_env_variable(var='GALAXY_fs_cell')
        else:
            return path.replace('/galaxy/', '').split('-')[0]

    @classmethod
    def get_cell_path_local_path(cls, path, cell=''):
        if cls.is_local_path(path) or '/galaxy/' not in path:
            return path
        try:
            this_cell = cell if cell else EnvUtil.get_other_env_variable(var='GALAXY_fs_cell')
            with open(EnvUtil.get_other_env_variable('GALAXY_fs_global_config'), 'r') as infile:
                config = json.load(infile)
                root = config[this_cell]['fs_root']
                if root and root[-1] != '/':
                    root += '/'
                return root + '/'.join(path.split('/')[3:])
        except Exception as _:
            return ''

    @classmethod
    def get_file_attr(cls, file_name):
        attr = gclient.get_attr(file_name)
        if attr:
            return json.loads(attr)
        else:
            return {}
