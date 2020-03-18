import time
from apscheduler.schedulers.background import BackgroundScheduler

from pslx.core.container_base import ContainerBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.tool.logging_tool import LoggingTool
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneObj, TimeSleepObj


class DefaultBatchContainer(ContainerBase):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, container_name, ttl=-1):
        super().__init__(container_name, ttl=ttl)
        self._logger = LoggingTool(
            name=(self.get_class_name() + '__' +
                  ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + '__' + container_name),
            ttl=ttl
        )
        self._config = {
            'max_instances': 1,
        }


class CronBatchContainer(DefaultBatchContainer):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, container_name, ttl=-1):
        super().__init__(container_name, ttl=ttl)
        self._scheduler_spec = {
            'day_of_week': None,
            'hour': None,
            'minute': None,
            'second': None,
        }

    def set_schedule(self, day_of_week, hour, minute=None, second=None):
        self._scheduler_spec = {
            'day_of_week': day_of_week,
            'hour': hour,
            'minute': minute,
            'second': second,
        }
        self._logger.write_log("Spec sets to " + str(self._scheduler_spec))
        self.sys_log("Spec sets to " + str(self._scheduler_spec))

    def _execute_wrapper(self, is_backfill, num_threads):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
        super().execute(is_backfill=is_backfill, num_threads=num_threads)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=TimezoneObj.WESTERN_TIMEZONE)
        background_scheduler.add_job(
            self._execute_wrapper,
            'cron',
            args=[is_backfill, num_threads],
            day_of_week=self._scheduler_spec['day_of_week'],
            hour=self._scheduler_spec['hour'],
            minute=self._scheduler_spec['minute'],
            second=self._scheduler_spec['second'],
            max_instances=self._config['max_instances'],
            misfire_grace_time=None
        )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()


class IntervalBatchContainer(DefaultBatchContainer):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, container_name, ttl=-1):
        super().__init__(container_name, ttl=ttl)
        self._scheduler_spec = {
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
        }

    def set_schedule(self, days, hours=0, minutes=0, seconds=0):
        self._scheduler_spec = {
            'days': days,
            'hours': hours,
            'minutes': minutes,
            'seconds': seconds,
        }
        self._logger.write_log("Spec sets to " + str(self._scheduler_spec))
        self.sys_log("Spec sets to " + str(self._scheduler_spec))

    def _execute_wrapper(self, is_backfill, num_threads):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
        super().execute(is_backfill=is_backfill, num_threads=num_threads)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=TimezoneObj.WESTERN_TIMEZONE)
        background_scheduler.add_job(
            self._execute_wrapper,
            'interval',
            args=[is_backfill, num_threads],
            days=self._scheduler_spec['days'],
            hours=self._scheduler_spec['hours'],
            minutes=self._scheduler_spec['minutes'],
            seconds=self._scheduler_spec['seconds'],
            max_instances=self._config['max_instances'],
            misfire_grace_time=None
        )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()
