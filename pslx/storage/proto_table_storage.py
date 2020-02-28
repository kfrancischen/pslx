import time

from pslx.core.exception import StorageReadException, StorageWriteException
from pslx.schema.enums_pb2 import StorageType, Status
from pslx.schema.storage_pb2 import ProtoTable
from pslx.storage.storage_base import StorageBase
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil, TimeSleepObj


class ProtoTableStorage(StorageBase):
    STORAGE_TYPE = StorageType.PROTO_TABLE_STORAGE

    def __init__(self, logger=None):
        super().__init__(logger=logger)
        self._file_name = None
        self._table_message = None

    def initialize_from_dir(self, dir_name):
        self.sys_log("Initialize_from_dir function is not implemented for storage type "
                     + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        return

    def initialize_from_file(self, file_name):
        if '.pb' not in file_name:
            self.sys_log("Please use .pb extension for proto files.")

        self._file_name = FileUtil.create_file_if_not_exist(file_name=file_name)
        if FileUtil.is_file_empty(file_name=self._file_name):
            self._table_message = ProtoTable()
        else:
            self._table_message = FileUtil.read_proto_from_file(
                proto_type=ProtoTable,
                file_name=self._file_name
            )
        if not self._table_message.table_path:
            self._table_message.table_path = self._file_name
        if not self._table_message.table_name:
            self._table_message.table_name = FileUtil.base_name(file_name=file_name)
        if not self._table_message.create_time:
            self._table_message.create_time = str(TimezoneUtil.cur_time_in_pst())

    def get_file_name(self):
        return self._file_name

    def read(self, params=None):
        assert 'message_type' in params and 'key' in params

        while self._writer_status != Status.IDLE:
            self.sys_log("Waiting for writer to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._reader_status = Status.RUNNING
        if params['key'] in self._table_message.data:
            try:
                result = ProtoUtil.any_to_message(
                    message_type=params['message_type'],
                    any_message=self._table_message.data[params['key']]
                )
                self._reader_status = Status.IDLE
                return result
            except Exception as err:
                self.sys_log("Got exception: " + str(err))
                self._logger.write_log("Got exception: " + str(err))
                raise StorageReadException
        else:
            return None

    def write(self, data, params=None):
        if not params:
            params = {}
        if 'overwrite' not in params:
            params['overwrite'] = True

        while self._reader_status != Status.IDLE:
            self.sys_log("Waiting for reader to finish.")
            time.sleep(TimeSleepObj.ONE_SECOND)

        self._writer_status = Status.RUNNING
        assert isinstance(data, list) and len(data) == 2 and isinstance(data[0], str)
        if data[0] in self._table_message.data and not params['overwrite']:
            self.sys_log(data[0] + " already exist and will not be overwritten.")
            self._logger.write_log(data[0] + " already exist and will not be overwritten.")
        else:
            try:
                any_message = ProtoUtil.message_to_any(message=data[1])
                self._table_message.data[data[0]].CopyFrom(any_message)
                self._table_message.updated_time = str(TimezoneUtil.cur_time_in_pst())
                FileUtil.write_proto_to_file(
                    proto=self._table_message,
                    file_name=self._file_name
                )
                self._writer_status = Status.IDLE
            except Exception as err:
                self.sys_log("Got exception: " + str(err))
                self._logger.write_log("Got exception: " + str(err))
                raise StorageWriteException
