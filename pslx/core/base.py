import os
from inspect import getmodule
from galaxy_py import glogging
from pslx.schema.enums_pb2 import ModeType
from pslx.util.dummy_util import DummyUtil
from pslx.util.env_util import EnvUtil


class Base(object):

    _SYS_LOGGER = (glogging.get_logger("PSLX_SYS_LOG", EnvUtil.get_pslx_env_variable("PSLX_SYS_LOG_DIR")) if
                   EnvUtil.get_pslx_env_variable("PSLX_ENABLE_SYS_LOG") else DummyUtil.dummy_logger())

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
