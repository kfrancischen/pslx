import time

from pslx.core.exception import StorageExceedsFixedSizeException, StorageWriteException
from pslx.schema.enums_pb2 import StorageType, Status, WriteRuleType
from pslx.storage.default_storage import DefaultStorage
from pslx.util.timezone_util import TimeSleepObj


class FixedSizeStorage(DefaultStorage):
    STORAGE_TYPE = StorageType.FIXED_SIZE_STORAGE

    def __init__(self, logger=None, fixed_size=-1):
        super().__init__(logger=logger)
        self._file_name = None
        self._fixed_size = fixed_size
        self._config = {
            'write_rule_type': WriteRuleType.WRITE_FROM_END,
        }
        self._stored_data = None

    def _pre_load_data(self):
        if not self._stored_data:
            self._stored_data = super(FixedSizeStorage, self).read(
                params={
                    'num_line': self._fixed_size
                }
            )

    def read(self, params=None):
        if not params:
            params = {
                'num_line': 1,
                'force_load': False,
            }
        else:
            assert isinstance(params, dict) and 'num_line' in params and 'force_load' in params

        for param in params:
            if param not in ['num_line', 'force_load']:
                self._logger.write_log(param +
                                       " will be omitted since it is not useful as an input argument in this function.")
                self.sys_log(param + " will be omitted since it is not useful as an input argument in this function.")

        while self._writer_status != Status.IDLE:
            self.sys_log("Waiting for writer to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._reader_status = Status.RUNNING

        self._pre_load_data()

        if params['num_line'] <= self._fixed_size:
            self._reader_status = Status.IDLE
            return self._stored_data[:params['num_line']]
        else:
            if params['force_load']:
                self._stored_data = super(FixedSizeStorage, self).read(
                    params={
                        'num_line': params['num_line']
                    }
                )
                self._reader_status = Status.IDLE
                return self._stored_data
            else:
                self._logger.write_log("Failed to read size of " + str(params['num_line']) + ' . Exceeds ' +
                                       str(self._fixed_size) + '!')
                self.sys_log("Failed to read size of " + str(params['num_line']) + ' . Exceeds ' +
                             str(self._fixed_size) + '!')
                raise StorageExceedsFixedSizeException

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
        self._pre_load_data()

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
        # (TODO)
        pass

