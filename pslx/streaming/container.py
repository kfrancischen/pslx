from apscheduler.schedulers.background import BackgroundScheduler
import time

from pslx.core.container_base import ContainerBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.tool.logging_tool import LoggingTool
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneObj, TimeSleepObj


class DefaultStreamingContainer(ContainerBase):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, ttl=-1):
        super().__init__(container_name, ttl=ttl)
        self._logger = LoggingTool(
            name=(ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + '__' +
                  self.get_class_name() + '__' + container_name),
            ttl=ttl
        )

    def execute(self, is_backfill=False, num_threads=1):
        self.sys_log(string='Streaming Container does not allow multi-processing.')
        super().execute(is_backfill=is_backfill, num_threads=1)


class CronStreamingContainer(DefaultStreamingContainer):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, ttl=-1):
        super().__init__(container_name, ttl=ttl)
        self._scheduler_specs = []

    def add_schedule(self, day_of_week, hour, minute=None, second=None):
        scheduler_spec = {
            'day_of_week': day_of_week,
            'hour': hour,
            'minute': minute,
            'second': second,
        }
        self._logger.info("Adding schedule spec: " + str(scheduler_spec))
        self.sys_log("Adding schedule spec: " + str(scheduler_spec))
        self._scheduler_specs.append(scheduler_spec)

    def _execute_wrapper(self):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
        super().execute(is_backfill=False, num_threads=1)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=TimezoneObj.WESTERN_TIMEZONE)
        for scheduler_spec in self._scheduler_specs:
            background_scheduler.add_job(
                self._execute_wrapper,
                'cron',
                day_of_week=scheduler_spec['day_of_week'],
                hour=scheduler_spec['hour'],
                minute=scheduler_spec['minute'],
                second=scheduler_spec['second']
            )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()


class IntervalStreamingContainer(DefaultStreamingContainer):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, ttl=-1):
        super().__init__(container_name, ttl=ttl)
        self._scheduler_specs = []

    def add_schedule(self, days, hours=0, minutes=0, seconds=0):
        scheduler_spec = {
            'days': days,
            'hours': hours,
            'minutes': minutes,
            'seconds': seconds,
        }
        self._scheduler_specs.append(scheduler_spec)
        self._logger.info("Spec sets to " + str(scheduler_spec))
        self.sys_log("Spec sets to " + str(scheduler_spec))

    def _execute_wrapper(self):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
        super().execute(is_backfill=False, num_threads=1)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=TimezoneObj.WESTERN_TIMEZONE)
        for scheduler_spec in self._scheduler_specs:
            background_scheduler.add_job(
                self._execute_wrapper,
                'interval',
                args=[is_backfill, num_threads],
                days=scheduler_spec['days'],
                hours=scheduler_spec['hours'],
                minutes=scheduler_spec['minutes'],
                seconds=scheduler_spec['seconds'],
            )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()
