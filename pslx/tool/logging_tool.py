import logging
import datetime
from inspect import getframeinfo, stack
import os
from pslx.core.base import Base
from pslx.schema.enums_pb2 import DiskLoggerLevel
from pslx.util.env_util import EnvUtil
from pslx.util.timezone_util import TimezoneUtil
from pslx.util.file_util import FileUtil


class LoggingTool(Base):
    def __init__(self, name, date=datetime.datetime.utcnow(), ttl=-1):
        super().__init__()
        if name:
            self._start_date = date
            assert '-' not in name

            self._name = name
            self._log_file_dir = FileUtil.join_paths_to_file_with_mode(
                root_dir=FileUtil.join_paths_to_dir(
                    root_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DATABASE'),
                    base_name='log'
                ),
                base_name=name,
                ttl=ttl
            )

            self._new_logger()
            self._level_to_caller_map = {
                DiskLoggerLevel.INFO: self._logger.info,
                DiskLoggerLevel.WARNING: self._logger.warning,
                DiskLoggerLevel.DEBUG: self._logger.debug,
                DiskLoggerLevel.ERROR: self._logger.error,
            }

    def _new_logger(self):
        logging.basicConfig(level=logging.INFO)
        self._logger = logging.getLogger(self._name)
        if not os.path.exists(self._log_file_dir):
            os.makedirs(self._log_file_dir)
        file_name = self._log_file_dir + '/' + self._name + '-' + str(self._start_date.year) + '-' + str(
            self._start_date.month) + '-' + str(self._start_date.day)

        fh = logging.FileHandler(file_name + '.log')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

    def _write_log(self, string, logger_level):
        now = datetime.datetime.utcnow()
        if now.date() != self._start_date.date():
            self._start_date = now
            self._new_logger()
        try:
            caller = getframeinfo(stack()[2][0])
            self._level_to_caller_map[logger_level](
                ' [' + FileUtil.base_name(caller.filename) + ': ' + str(caller.lineno) + ', ' +
                str(TimezoneUtil.cur_time_in_pst().replace(tzinfo=None)) + ' PST]: ' + string)
        except Exception as _:
            self._level_to_caller_map[logger_level](' [' + str(TimezoneUtil.cur_time_in_pst().replace(tzinfo=None)) +
                                                    ' PST]: ' + string)

    def info(self, string):
        self._write_log(string=string, logger_level=DiskLoggerLevel.INFO)

    def warning(self, string):
        self._write_log(string=string, logger_level=DiskLoggerLevel.WARNING)

    def debug(self, string):
        self._write_log(string=string, logger_level=DiskLoggerLevel.DEBUG)

    def error(self, string):
        self._write_log(string=string, logger_level=DiskLoggerLevel.ERROR)
