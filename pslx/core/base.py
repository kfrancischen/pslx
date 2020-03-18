import os
from inspect import getmodule, getframeinfo, stack
from pslx.schema.enums_pb2 import ModeType
from pslx.util.color_util import ColorsUtil
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.timezone_util import TimezoneUtil


class Base(object):

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

    @classmethod
    def sys_log(cls, string):
        if EnvUtil.get_pslx_env_variable(var='PSLX_LOG'):
            try:
                caller = getframeinfo(stack()[1][0])
                print('[SYS-LOG] ' + ColorsUtil.Foreground.GREEN + '[file: %s]' % FileUtil.base_name(caller.filename) +
                      ColorsUtil.RESET + ' ' + ColorsUtil.Foreground.YELLOW + '[line: %d]' % caller.lineno +
                      ColorsUtil.RESET + ' ' + ColorsUtil.Foreground.RED + '[%s]' % str(TimezoneUtil.cur_time_in_pst())
                      + ColorsUtil.RESET + ': ' + string)
            except Exception as _:
                print('[SYS-LOG] ' + ColorsUtil.Foreground.GREEN + ColorsUtil.RESET + ' ' + ColorsUtil.Foreground.RED +
                      '[%s]' % str(TimezoneUtil.cur_time_in_pst()) + ColorsUtil.RESET + ': ' + string)
