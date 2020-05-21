import datetime
import time
from pslx.core.base import Base
from pslx.micro_service.rpc_io.client import PartitionerStorageRPC
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneUtil, TimeSleepObj


class LocalPartitionerWatcher(Base):
    MESSAGE_TYPE = None

    def __init__(self, partitioner, logger=DummyUtil.dummy_logging(), delay=TimeSleepObj.ONE_TENTH_SECOND, timeout=-1):
        super().__init__()
        self._partitioner = partitioner
        self._timeout = timeout
        self._delay = delay
        self._logger = logger

    def watch_key(self, key):
        start_time = TimezoneUtil.cur_time_in_pst()
        delay = self._delay
        while (self._timeout < 0 or
               TimezoneUtil.cur_time_in_pst() - start_time < datetime.timedelta(seconds=self._timeout)):
            try:
                value = self._partitioner.read(
                    params={
                        'key': key,
                        'message_type': self.MESSAGE_TYPE,
                        'reinitialize_underlying_storage': True,
                    }
                )
                if value:
                    self._logger.info("Successfully got data for key [" + key + "] in partitioner dir [" +
                                      self._partitioner.get_dir_name() + '].')
                    return value
                else:
                    self._logger.warning("Trying to fetch data for key [" + key + "] again in partitioner dir [" +
                                         self._partitioner.get_dir_name() + '].')
                    delay = self._delay if delay < TimeSleepObj.ONE_HUNDREDTH_SECOND else delay / 2.0
                    time.sleep(delay)
            except Exception as err:
                self._logger.error("Read partition [" + self._partitioner.get_dir_name() +
                                   "] with error " + str(err) + '.')

        return None


class RemotePartitionerWatcher(Base):
    MESSAGE_TYPE = None
    PARTITIONER_STORAGE_TYPE = None

    def __init__(self, partitioner_dir, server_url, logger=DummyUtil.dummy_logging(),
                 root_certificate=None, delay=TimeSleepObj.ONE_TENTH_SECOND, timeout=-1):
        super().__init__()
        self._partitioner_dir = partitioner_dir
        self._timeout = timeout
        self._delay = delay
        self._server_url = server_url
        self._root_certificate = root_certificate
        self._rpc_client = PartitionerStorageRPC(
            client_name="remote_partitioner_watcher",
            server_url=server_url
        )
        self._logger = logger

    def watch_key(self, key):
        start_time = TimezoneUtil.cur_time_in_pst()
        delay = self._delay
        while (self._timeout < 0 or
               TimezoneUtil.cur_time_in_pst() - start_time < datetime.timedelta(seconds=self._timeout)):
            all_data = self._rpc_client.read(
                file_or_dir_path=self._partitioner_dir,
                params={
                    'PartitionerStorageType': self.PARTITIONER_STORAGE_TYPE,
                    'is_proto_table': True,
                },
                root_certificate=self._root_certificate
            )
            if key in all_data:
                self._logger.info("Successfully get the data for key [" + str(key) + '] in partition dir [' +
                                  self._partitioner_dir + '].')
                return ProtoUtil.any_to_message(
                    message_type=self.MESSAGE_TYPE,
                    any_message=all_data[key]
                )
            else:
                self._logger.warning("Trying to fetch data for key [" + key + "] again in partitioner dir [" +
                                     self._partitioner_dir + '].')
                delay = self._delay if delay < TimeSleepObj.ONE_HUNDREDTH_SECOND else delay / 2.0
                time.sleep(delay)

        return None
