from apscheduler.schedulers.background import BackgroundScheduler
import time
from galaxy_py import glogging

from pslx.core.container_base import ContainerBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneObj, TimeSleepObj


class DefaultStreamingContainer(ContainerBase):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name):
        super().__init__(container_name)
        self._logger = glogging.get_logger(
            log_name=(ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + '__' +
                      self.get_class_name() + '__' + container_name),
            log_dir=EnvUtil.get_pslx_env_variable('PSLX_DEFAULT_LOG_DIR')
        )

    def execute(self, is_backfill=False, num_threads=1):
        self._SYS_LOGGER.info(string='Streaming Container does not allow multi-processing.')
        super().execute(is_backfill=is_backfill, num_threads=1)


class CronStreamingContainer(DefaultStreamingContainer):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, timezone=TimezoneObj.WESTERN_TIMEZONE):
        super().__init__(container_name)
        self._scheduler_specs = []
        self._timezone = timezone

    def add_schedule(self, day_of_week, hour, minute=None, second=None, misfire_grace_time=None):
        scheduler_spec = {
            'day_of_week': day_of_week,
            'hour': hour,
            'minute': minute,
            'second': second,
            'misfire_grace_time': misfire_grace_time,
        }
        self._logger.info("Adding schedule spec: " + str(scheduler_spec))
        self._SYS_LOGGER.info("Adding schedule spec: " + str(scheduler_spec))
        self._scheduler_specs.append(scheduler_spec)

    def _execute_wrapper(self):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
            node.unset_content()
        self.unset_counters()
        super().execute(is_backfill=False, num_threads=1)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=self._timezone)
        for scheduler_spec in self._scheduler_specs:
            background_scheduler.add_job(
                self._execute_wrapper,
                'cron',
                day_of_week=scheduler_spec['day_of_week'],
                hour=scheduler_spec['hour'],
                minute=scheduler_spec['minute'],
                second=scheduler_spec['second'],
                misfire_grace_time=scheduler_spec['misfire_grace_time']
            )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()


class IntervalStreamingContainer(DefaultStreamingContainer):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, timezone=TimezoneObj.WESTERN_TIMEZONE):
        super().__init__(container_name)
        self._scheduler_specs = []
        self._timezone = timezone

    def add_schedule(self, days, hours=0, minutes=0, seconds=0, misfire_grace_time=None):
        scheduler_spec = {
            'days': days,
            'hours': hours,
            'minutes': minutes,
            'seconds': seconds,
            'misfire_grace_time': misfire_grace_time,
        }
        self._scheduler_specs.append(scheduler_spec)
        self._logger.info("Spec sets to " + str(scheduler_spec))
        self._SYS_LOGGER.info("Spec sets to " + str(scheduler_spec))

    def _execute_wrapper(self):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
            node.unset_content()
        self.unset_counters()
        super().execute(is_backfill=False, num_threads=1)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=self._timezone)
        for scheduler_spec in self._scheduler_specs:
            background_scheduler.add_job(
                self._execute_wrapper,
                'interval',
                args=[is_backfill, num_threads],
                days=scheduler_spec['days'],
                hours=scheduler_spec['hours'],
                minutes=scheduler_spec['minutes'],
                seconds=scheduler_spec['seconds'],
                misfire_grace_time=scheduler_spec['misfire_grace_time']
            )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()


class NonStoppingStreamingContainer(DefaultStreamingContainer):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name):
        super().__init__(container_name)

    def _execute_wrapper(self):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
            node.unset_content()
        self.unset_counters()
        super().execute(is_backfill=False, num_threads=1)

    def execute(self, is_backfill=False, num_threads=1, sleep_between_execution=TimeSleepObj.HALF_SECOND):
        while True:
            self._logger.info("Entering executing loop. Starting one execution...")
            self._execute_wrapper()
            if sleep_between_execution > 0:
                time.sleep(sleep_between_execution)
