from pslx.core.exception import StorageReadException, StorageWriteException, StorageDeleteException
from pslx.schema.enums_pb2 import StorageType
from pslx.schema.storage_pb2 import ProtoTable
from pslx.storage.storage_base import StorageBase
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil


class ProtoTableStorage(StorageBase):
    STORAGE_TYPE = StorageType.PROTO_TABLE_STORAGE

    def __init__(self, logger=None):
        super().__init__(logger=logger)
        self._file_name = None
        self._table_message = None

    def initialize_from_dir(self, dir_name):
        self._SYS_LOGGER.fatal("Initialize_from_dir function is not implemented for storage type "
                               + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        return

    def initialize_from_file(self, file_name):
        if '.pb' not in file_name:
            self._SYS_LOGGER.warning("Please use .pb extension for proto files.")

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
        if not self._table_message.created_time:
            self._table_message.created_time = str(TimezoneUtil.cur_time_in_pst())

    def get_file_name(self):
        return self._file_name

    def get_num_entries(self):
        return len(self._table_message.data)

    def read(self, params):
        assert 'key' in params
        try:
            result = self.read_multiple(params={'keys': [params['key']]})
            value = list(result.values())[0]
            if 'message_type' in params:
                value = ProtoUtil.any_to_message(
                    message_type=params['message_type'],
                    any_message=value
                )

            return value
        except Exception as err:
            self._SYS_LOGGER.error("Read file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Read file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
            return None

    def read_multiple(self, params):
        assert 'keys' in params
        result = {}
        for key in params['keys']:
            if key in self._table_message.data:
                result[key] = self._table_message.data[key]
        return result

    def read_all(self):
        try:
            return dict(self._table_message.data)
        except Exception as err:
            self._SYS_LOGGER.error("Read all got exception: " + str(err) + '.')
            self._logger.error("Read all Got exception: " + str(err) + '.')
            raise StorageReadException

    def delete(self, key):
        try:
            self.delete_multiple(keys=[key])
        except Exception as err:
            self._SYS_LOGGER.error("Delete file [" + self.get_file_name() + "] got exception: " + str(err))
            self._logger.error("Delete file [" + self.get_file_name() + "] got exception: " + str(err))
            raise StorageDeleteException("Delete file [" + self.get_file_name() + "] got exception: " + str(err))

    def delete_multiple(self, keys):
        for key in keys:
            if key in self._table_message.data:
                del self._table_message.data[key]

        try:
            self._table_message.updated_time = str(TimezoneUtil.cur_time_in_pst())
            FileUtil.write_proto_to_file(
                proto=self._table_message,
                file_name=self._file_name
            )
        except Exception as err:
            self._SYS_LOGGER.error("Delete file [" + self.get_file_name() + "] got exception: " + str(err))
            self._logger.error("Delete file [" + self.get_file_name() + "] got exception: " + str(err))
            raise StorageDeleteException("Delete file [" + self.get_file_name() + "] got exception: " + str(err))

    def delete_all(self):

        all_keys = list(dict(self._table_message.data).keys())
        for key in all_keys:
            del self._table_message.data[key]
        try:
            self._table_message.updated_time = str(TimezoneUtil.cur_time_in_pst())
            FileUtil.write_proto_to_file(
                proto=self._table_message,
                file_name=self._file_name
            )

        except Exception as err:
            self._SYS_LOGGER.error("Delete all of file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Delete all of file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
            raise StorageDeleteException("Delete all of file [" + self.get_file_name() +
                                         "] got exception: " + str(err) + '.')

    def write(self, data, params=None):
        if not params:
            params = {}
        if 'overwrite' not in params:
            params['overwrite'] = True

        assert isinstance(data, dict)
        try:
            for key, val in data.items():
                if not params['overwrite'] and key in self._table_message.data:
                    continue
                any_message = ProtoUtil.message_to_any(message=val)
                self._table_message.data[key].CopyFrom(any_message)
            if len(self._table_message.data) > 1000:
                self._SYS_LOGGER.warning("Warning: the table content is too large, considering using Partitioner "
                                         "combined with proto table.")
            self._table_message.updated_time = str(TimezoneUtil.cur_time_in_pst())

            FileUtil.write_proto_to_file(
                proto=self._table_message,
                file_name=self._file_name
            )

        except Exception as err:
            self._SYS_LOGGER.error("Write to file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Write to file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
            raise StorageWriteException("Write to file [" + self.get_file_name() + "] got exception: " + str(err) + '.')
