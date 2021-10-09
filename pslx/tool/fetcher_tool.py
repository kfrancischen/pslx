from pslx.core.base import Base
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.util.dummy_util import DummyUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


class PartitionerFetcher(Base):
    MESSAGE_TYPE = None

    def __init__(self, partitioner, logger=DummyUtil.dummy_logger()):
        super().__init__()
        self._partitioner = partitioner
        self._logger = logger

    def fetch_latest(self):
        try:
            latest_dir = self._partitioner.get_latest_dir()
            if not latest_dir:
                self._logger.warning('[' + self._partitioner.get_dir_name() + '] is empty.')
                return None
            proto_table = ProtoTableStorage()
            proto_table.initialize_from_file(
                file_name=FileUtil.join_paths_to_file(
                    root_dir=latest_dir,
                    base_name='data.pb'
                )
            )
            all_data = proto_table.read_all()
            if all_data:
                self._logger.info("Successfully get the latest data in partition dir [" +
                                  self._partitioner.get_dir_name() + '].')
                max_key = max(all_data.keys())
                return ProtoUtil.any_to_message(
                    message_type=self.MESSAGE_TYPE,
                    any_message=all_data[max_key]
                )
            else:
                return None
        except Exception as err:
            self._logger.error("Fetch latest partition [" + self._partitioner.get_dir_name() +
                               "] with error " + str(err) + '.')
        return None

    def fetch_oldest(self):
        try:
            oldest_dir = self._partitioner.get_oldest_dir_in_root_directory()
            if not oldest_dir:
                self._logger.warning('[' + self._partitioner.get_dir_name() + '] is empty.')
                return None
            proto_table = ProtoTableStorage()
            proto_table.initialize_from_file(
                file_name=FileUtil.join_paths_to_file(
                    root_dir=oldest_dir,
                    base_name='data.pb'
                )
            )
            all_data = proto_table.read_all()
            if all_data:
                self._logger.info("Successfully get the oldest data in partition dir [" +
                                  self._partitioner.get_dir_name() + '].')
                min_key = min(all_data.keys())
                return ProtoUtil.any_to_message(
                    message_type=self.MESSAGE_TYPE,
                    any_message=all_data[min_key]
                )
            else:
                return None
        except Exception as err:
            self._logger.error("Fetch oldest partition [" + self._partitioner.get_dir_name() +
                               "] with error " + str(err) + '.')
        return None

    def fetch_range(self, start_time, end_time):
        try:
            data = self._partitioner.read_range(
                params={
                    'start_time': start_time,
                    'end_time': end_time,
                }
            )
            start_time, end_time = str(start_time.replace(tzinfo=None)), str(end_time.replace(tzinfo=None))
            data_content = {}
            for val in data.values():
                data_content.update(val)
            result = {}
            self._logger.info("Successfully get the range data in partition dir [" +
                              self._partitioner.get_dir_name() + '].')
            for key in sorted(data_content.keys()):
                if key < start_time or key > end_time:
                    continue
                else:
                    item = ProtoUtil.any_to_message(
                        message_type=self.MESSAGE_TYPE,
                        any_message=data_content[key]
                    )
                    result[key] = item
            return result
        except Exception as err:
            self._logger.error("Fetch range for partition [" + self._partitioner.get_dir_name() +
                               "] with error " + str(err) + '.')
        return {}

    def get_rpc_call_count_and_reset(self):
        return self._partitioner.get_rpc_call_count_and_reset()

    def get_rpc_call_count(self):
        return self._partitioner.get_rpc_call_count()
