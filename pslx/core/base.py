import os
from inspect import getmodule
from galaxy_py import glogging
from pslx.schema.enums_pb2 import ModeType
from pslx.util.env_util import EnvUtil


class DummyLogger(object):
    def __init__(self):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def fatal(self, *args, **kwargs):
        pass

    def critical(self, *args, **kwargs):
        pass


class Base(object):

    _SYS_LOGGER = (glogging.get_logger("PSLX_SYS_LOG", EnvUtil.get_pslx_env_variable("PSLX_DEFAULT_LOG_DIR") +
                                       'sys_log/') if
                   EnvUtil.get_pslx_env_variable("PSLX_ENABLE_SYS_LOG") else DummyLogger())

    def __init__(self):
        if 'PSLX_TEST' not in os.environ or not os.environ['PSLX_TEST']:
            self._mode = ModeType.PROD
        else:
            self._mode = ModeType.TEST
        self._config = {}

    def get_mode(self):
        return self._mode

    @classmethod
    def get_class_name(cls):
        return cls.__name__

    def set_config(self, config):
        assert isinstance(config, dict)
        self._config.update(config)

    @classmethod
    def get_full_class_name(cls):
        file_path = getmodule(cls).__name__
        return '.'.join(file_path.replace('.py', '').split('/') + [cls.__name__])

    @classmethod
    def get_inheritance_level(cls):
        mro = cls.mro()
        inheritance_level = []
        for class_obj in mro[::-1][1:]:
            inheritance_level.append(class_obj.__name__)
        return '->'.join(inheritance_level)
