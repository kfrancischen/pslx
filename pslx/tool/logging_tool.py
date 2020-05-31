import logging
import datetime
from inspect import getframeinfo, stack
import traceback
from pslx.schema.enums_pb2 import DiskLoggerLevel
from pslx.schema.rpc_pb2 import LoggingMessageRequest
from pslx.util.color_util import ColorsUtil
from pslx.util.env_util import EnvUtil
from pslx.util.timezone_util import TimezoneUtil
from pslx.util.file_util import FileUtil


class LoggingTool(object):
    def __init__(self, name, date=datetime.datetime.utcnow(),
                 ttl=EnvUtil.get_pslx_env_variable(var="PSLX_INTERNAL_TTL")):
        self._log_file_dir = ''
        self._publisher = None
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
            self._level_to_logger_level_map = {
                DiskLoggerLevel.INFO: logging.INFO,
                DiskLoggerLevel.WARNING: logging.WARNING,
                DiskLoggerLevel.DEBUG: logging.DEBUG,
                DiskLoggerLevel.ERROR: logging.ERROR,
            }
            self._level_to_logger_short_level_map = {
                DiskLoggerLevel.INFO: 'I',
                DiskLoggerLevel.WARNING: 'W',
                DiskLoggerLevel.DEBUG: 'D',
                DiskLoggerLevel.ERROR: 'E',
            }

    def get_log_dir(self):
        return self._log_file_dir

    def set_publisher(self, publisher):
        self._publisher = publisher

    def _new_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='[%(levelname).1s %(name)s %(message)s'
        )
        self._logger = logging.getLogger(self._name)
        FileUtil.create_dir_if_not_exist(dir_name=self._log_file_dir)
        file_name = FileUtil.join_paths_to_file(
            root_dir=self._log_file_dir,
            base_name=self._name + '-' + str(self._start_date.year) + '-' + str(self._start_date.month) + '-' + str(
                self._start_date.day) + '.log'
        )

        fh = logging.FileHandler(file_name)
        for handler in self._logger.handlers:
            self._logger.removeHandler(handler)

        formatter = logging.Formatter('[%(levelname).1s %(name)s %(message)s')
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

    def _write_log(self, string, logger_level, publish):
        now = datetime.datetime.utcnow()
        if now.date() != self._start_date.date():
            self._start_date = now
            self._new_logger()

        try:
            caller = getframeinfo(stack()[2][0])
            message = FileUtil.base_name(caller.filename) + ':' + str(caller.lineno) + ' ' + str(
                TimezoneUtil.cur_time_in_pst()) + '] '

        except Exception as _:
            message = str(TimezoneUtil.cur_time_in_pst()) + '] '

        if logger_level == DiskLoggerLevel.WARNING:
            message += ColorsUtil.make_foreground_yellow(text=string)
        elif logger_level == DiskLoggerLevel.ERROR:
            message += ColorsUtil.make_foreground_red(
                text=string + '\n\nENTERING TRACEBACK:\n' + traceback.format_exc() + '.')
        else:
            message += ColorsUtil.make_foreground_green(text=string)

        self._logger.setLevel(level=self._level_to_logger_level_map[logger_level])
        self._level_to_caller_map[logger_level](message)
        message_proto = LoggingMessageRequest()
        message_proto.message = ('[' + self._level_to_logger_short_level_map[logger_level] + ' ' + self._name +
                                 ' ' + message)
        message_proto.level = logger_level
        if self._publisher and publish:
            self._publisher.publish(message=message_proto)

    def info(self, string, publish=False):
        self._write_log(string=string, logger_level=DiskLoggerLevel.INFO, publish=publish)

    def warning(self, string, publish=False):
        self._write_log(string=string, logger_level=DiskLoggerLevel.WARNING, publish=publish)

    def debug(self, string, publish=False):
        self._write_log(string=string, logger_level=DiskLoggerLevel.DEBUG, publish=publish)

    def error(self, string, publish=False):
        self._write_log(string=string, logger_level=DiskLoggerLevel.ERROR, publish=publish)


class DummyLogging(LoggingTool):
    def __init__(self, name=None, date=None, ttl=-0):
        super().__init__(name=None, date=date, ttl=ttl)

    def _write_log(self, string, logger_level, publish=False):
        return
