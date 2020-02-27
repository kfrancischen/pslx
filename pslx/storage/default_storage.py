import time

from pslx.core.exception import StoragePastLineException, StorageReadException, StorageWriteException
from pslx.schema.enums_pb2 import StorageType, ReadRuleType, WriteRuleType, Status
from pslx.schema.enums_pb2 import ModeType
from pslx.storage.storage_base import StorageBase
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimeSleepObj


class DefaultStorage(StorageBase):
    STORAGE_TYPE = StorageType.DEFAULT_STORAGE

    def __init__(self, logger=None):
        super().__init__(logger=logger)
        self._file_name = None
        self._config = {
            'read_rule_type': ReadRuleType.READ_FROM_BEGINNING,
            'write_rule_type': WriteRuleType.WRITE_FROM_END,
        }
        self._last_read_line = 0

    def initialize_from_dir(self, dir_name):
        self.sys_log("Initialize_from_dir function is not implemented for storage type "
                     + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        return

    def initialize_from_file(self, file_name):
        self._file_name = FileUtil.create_file_if_not_exist(file_name=file_name)
        self._last_read_line = 0

    def get_file_name(self):
        return self._file_name

    def set_config(self, config):
        super().set_config(config=config)
        if 'override_to_prod':
            self._file_name = self._file_name.replace(
                ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.TEST),
                ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.PROD)
            )
        if 'override_to_test':
            self._file_name = self._file_name.replace(
                ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.PROD),
                ProtoUtil.get_name_by_value(enum_type=ModeType, value=ModeType.TEST)
            )

    def start_from_first_line(self):
        self._last_read_line = 0

    def read(self, params=None):
        if not params:
            params = {
                'num_line': 1,
            }
        else:
            assert isinstance(params, dict) and 'num_line' in params

        for param in params:
            if param != 'num_line':
                self._logger.write_log(param +
                                       " will be omitted since it is not useful as an input argument in this function.")
                self.sys_log(param + " will be omitted since it is not useful as an input argument in this function.")

        while self._writer_status != Status.IDLE:
            self.sys_log("Waiting for writer to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._reader_status = Status.RUNNING
        with open(self._file_name, 'r') as infile:
            lines = infile.readlines()
            if 'read_rule_type' in self._config and self._config['read_rule_type'] == ReadRuleType.READ_FROM_END:
                lines = lines[::-1]

            new_line_number = self._last_read_line + params['num_line']
            if new_line_number > len(lines):
                self.sys_log(str(new_line_number) + " exceeds the file limit. " + self.get_file_name() + " only has " +
                             str(len(lines)) + " lines.")
                self._logger.write_log(str(new_line_number) + " exceeds the file limit. " + self.get_file_name() +
                                       " only has " + str(len(lines)) + " lines.")
                raise StoragePastLineException
            else:
                try:
                    result = lines[self._last_read_line:new_line_number]
                    assert len(result) == params['num_line']
                    result = [line.strip() for line in result]
                    self._last_read_line = new_line_number
                    self._reader_status = Status.IDLE
                    return result
                except Exception as err:
                    self.sys_log(str(err))
                    self._logger.write_log(str(err))
                    raise StorageReadException

    def write(self, data, params=None):
        if not isinstance(data, str):
            if not params:
                params = {
                    'delimiter': ','
                }
            else:
                assert isinstance(params, dict) and 'delimiter' in params

        if params:
            for param in params:
                if not isinstance(data, str) and param == 'delimiter':
                    continue
                self._logger.write_log(param +
                                       " will be omitted since it is not useful as an input argument in this function.")
                self.sys_log(param + " will be omitted since it is not useful as an input argument in this function.")

        while self._reader_status != Status.IDLE:
            self.sys_log("Waiting for reader to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._writer_status = Status.RUNNING
        if not isinstance(data, str):
            data_to_write = params['delimiter'].join([str(val) for val in data])
        else:
            data_to_write = data
        try:
            if self._config['write_rule_type'] == WriteRuleType.WRITE_FROM_END:
                with open(self._file_name, 'a') as outfile:
                    outfile.write(data_to_write + '\n')
            else:
                with open(self._file_name, 'r+') as outfile:
                    file_data = outfile.read()
                    outfile.seek(0, 0)
                    outfile.write(data_to_write + '\n' + file_data)

            self._writer_status = Status.IDLE
        except Exception as err:
            self.sys_log(str(err))
            self._logger.write_log(str(err))
            raise StorageWriteException
