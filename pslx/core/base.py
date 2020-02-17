import os
import inspect
from pslx.schema.enums_pb2 import ModeType
from pslx.util.timezone_util import TimezoneUtil
from pslx.util.color_util import ColorsUtil


class Base(object):
    LOG_EVERYTHING = os.getenv('PSLX_LOG', False)

    def __init__(self):
        if 'PSLX_TEST' not in os.environ or not os.environ['PSLX_TEST']:
            self._mode = ModeType.PROD
        else:
            self._mode = ModeType.TEST
        return

    def get_mode(self):
        return self._mode

    @classmethod
    def get_class_name(cls):
        return cls.__name__

    @classmethod
    def get_full_class_name(cls):
        file_path = inspect.getmodule(cls).__name__
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
        if cls.LOG_EVERYTHING:
            print('[SYS-LOG]' + ColorsUtil.BOLD + ' class' + ColorsUtil.RESET + ' ' +
                  ColorsUtil.Foreground.GREEN + '[%s]' % cls.get_class_name() + ColorsUtil.RESET + ' & ' +
                  ColorsUtil.BOLD + 'Timestamp' + ColorsUtil.RESET + ' ' +
                  ColorsUtil.Foreground.RED + '[%s]' % str(TimezoneUtil.cur_time_in_pst()) + ColorsUtil.RESET +
                  ': ' + string)
