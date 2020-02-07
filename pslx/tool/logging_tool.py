import logging
import datetime
import os
from pslx.core.base import Base
from pslx.config.general_config import TimeZoneObj, SearchDirObj
from pslx.schema.enums_pb2 import DiskLoggerLevel
from pslx.util.yaml_util import yaml_to_dict
from pslx.util.color_util import ColorsUtil


class LoggingTool(Base):
    assert SearchDirObj.CONFIG_DIR[-1] == '/'
    CONFIG_PATH = SearchDirObj.CONFIG_DIR + 'tool_config.yaml'

    def __init__(self, name, date=datetime.datetime.utcnow(), root_dir='database/', level=DiskLoggerLevel.INFO,
                 retention=0):
        super().__init__()
        if name:
            self._start_date = date
            assert '-' not in name

            if root_dir and root_dir[-1] != '/':
                root_dir += '/'

            retention = retention if retention > 0 else 0
            self._name = name

            if level == DiskLoggerLevel.DEBUG:
                self._logging_level = logging.DEBUG
                self._suffix = 'debug'
                self._bg_color = ColorsUtil.Background.ORANGE
            elif level == DiskLoggerLevel.INFO:
                self._logging_level = logging.INFO
                self._suffix = 'info'
                self._bg_color = ColorsUtil.Background.GREEN
            elif level == DiskLoggerLevel.WARNING:
                self._logging_level = logging.WARNING
                self._suffix = 'warning'
                self._bg_color = ColorsUtil.Background.RED
            else:
                self._logging_level = logging.NOTSET
                self._suffix = 'notset'
                self._bg_color = ColorsUtil.Background.PURPLE

            self._retention = retention
            self._log_file_dir = (root_dir + yaml_to_dict(file_name=self.CONFIG_PATH)['LOGGING_TOOL'][
                'DISK_LOG_FILE_DIR'] + 'ttl=' + str(self._retention) + '/')

            self._new_logger()

    def _new_logger(self):
        logging.basicConfig(level=self._logging_level)
        self._logger = logging.getLogger(self._name)
        if not os.path.exists(self._log_file_dir):
            os.makedirs(self._log_file_dir)
        file_name = self._log_file_dir + '/' + self._name + '-' + str(self._start_date.year) + '-' + str(
            self._start_date.month) + '-' + str(self._start_date.day) + '-' + self._suffix

        if self._retention > 0:
            for existing_file_name in os.listdir(self._log_file_dir):
                existing_file_name_split = existing_file_name.split('-')
                if existing_file_name_split[0] == self._name:
                    file_date = datetime.datetime(
                        int(existing_file_name_split[1]), int(existing_file_name_split[2]),
                        int(existing_file_name_split[3])
                    )
                    if self._start_date - file_date > datetime.timedelta(days=self._retention):
                        os.remove(self._log_file_dir + '/' + existing_file_name)

        fh = logging.FileHandler(file_name + '.log')
        fh.setLevel(self._logging_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

    def write_log(self, string):
        now = datetime.datetime.utcnow()
        if now.date() != self._start_date.date():
            self._start_date = now
            self._new_logger()

        self._logger.info(self._bg_color + self._suffix.upper() + ' [' +
                          str(datetime.datetime.now(TimeZoneObj.WESTERN_TIMEZONE).replace(tzinfo=None)) + '] ' +
                          ColorsUtil.RESET + string)
