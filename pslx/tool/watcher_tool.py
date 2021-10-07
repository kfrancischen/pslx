import datetime
import time
from pslx.core.base import Base
from pslx.util.dummy_util import DummyUtil
from pslx.util.timezone_util import TimezoneUtil, TimeSleepObj


class PartitionerWatcher(Base):
    MESSAGE_TYPE = None

    def __init__(self, partitioner, logger=DummyUtil.dummy_logger(), delay=TimeSleepObj.ONE_TENTH_SECOND, timeout=-1):
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

    def get_rpc_call_count_and_reset(self):
        return self._partitioner.get_rpc_call_count_and_reset()
