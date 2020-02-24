import time

from pslx.core.exception import StoragePastLineException
from pslx.schema.enums_pb2 import StorageType, ReadRuleType, WriteRuleType, Status
from pslx.storage.storage_base import StorageBase
from pslx.util.proto_util import ProtoUtil
from pslx.util.file_util import FileUtil
from pslx.util.timezone_util import TimeSleepObj


class DefaultStorage(StorageBase):
    STORAGE_TYPE = StorageType.DEFAULT_STORAGE

    def __init__(self, logger=None):
        super().__init__(logger=logger)
        self._file_name = None
        self._config = {
            'read_rule_type': ReadRuleType.READ_FROM_BEGINNING,
            'write_rule_type': WriteRuleType.WRITE_FROM_BEGINNING,
        }
        self._last_read_line = 0

    def initialize_from_dir(self, dir_name):
        self.sys_log("Initialize_from_dir function is not implemented for storage type "
                     + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        return

    def initialize_from_file(self, file_name):
        self._file_name = FileUtil.create_if_not_exist(file_name=file_name)

    def read(self, params=None):
        if not params:
            params = {
                'num_line': 1,
            }
        for param in params:
            if param != 'num_line':
                self.sys_log(param + " will be omitted since it is not useful as an input argument in this function.")

        while self._writer_status != Status.IDLE:
            self.sys_log("Waiting for writer to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._reader_status = Status.RUNNING
        with open(self._file_name, 'r') as infile:
            lines = infile.readlines()
            if self._config['read_rule_type'] == ReadRuleType.READ_FROM_END:
                lines = lines[::-1]

            new_line_number = self._last_read_line + params['num_line']
            if new_line_number > len(lines):
                raise StoragePastLineException
            else:
                result = lines[self._last_read_line:new_line_number]
                assert len(result) == params['num_line']
                self._last_read_line = new_line_number
                self._reader_status = Status.IDLE
                return result

    def write(self, data, params=None):
        assert isinstance(data, list)
        if not params:
            params = {
                'delimiter': ','
            }
        for param in params:
            if param != 'delimiter':
                self.sys_log(param + " will be omitted since it is not useful as an input argument in this function.")

        while self._reader_status != Status.IDLE:
            self.sys_log("Waiting for reader to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._writer_status = Status.RUNNING
        data_to_write = params['delimiter'].join(data)
        if self._config['write_rule_type'] == WriteRuleType.WRITE_FROM_BEGINNING:
            with open(self._file_name, 'a') as outfile:
                outfile.write(data_to_write + '\n')
        else:
            with open(self._file_name, 'r+') as outfile:
                file_data = outfile.read()
                outfile.seek(0, 0)
                outfile.write(data_to_write + '\n' + file_data)

        self._writer_status = Status.IDLE
