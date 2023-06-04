import time
from apscheduler.schedulers.background import BackgroundScheduler
from galaxy_py import glogging

from pslx.core.container_base import ContainerBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.common_pb2 import BatchContainerFinishMessage
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimezoneObj, TimeSleepObj


class DefaultBatchContainer(ContainerBase):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, container_name):
        super().__init__(container_name)
        self._logger = glogging.get_logger(
            log_name=(ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + '__' +
                      self.get_class_name() + '__' + container_name),
            log_dir=EnvUtil.get_pslx_env_variable('PSLX_DEFAULT_LOG_DIR')
        )
        self._config = {
            'max_instances': 1,
        }

    def format_finish_message(self):
        snapshot = self.get_container_snapshot()
        finish_message = BatchContainerFinishMessage(snapshot=snapshot)
        finish_message.end_time = snapshot.end_time
        for publisher in self._publishers:
            finish_message.topic_name = publisher.get_topic_name()
            finish_message.exchange_name = publisher.get_exchange_name()
            publisher.publish(finish_message)
        return finish_message


class CronBatchContainer(DefaultBatchContainer):
    DATA_MODEL = DataModelType.BATCH

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

    def _execute_wrapper(self, is_backfill, num_threads):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
            node.unset_content()
        self.unset_counters()
        super().execute(is_backfill=is_backfill, num_threads=num_threads)

    def execute(self, is_backfill=False, num_threads=1):
        background_scheduler = BackgroundScheduler(timezone=self._timezone)
        for scheduler_spec in self._scheduler_specs:
            background_scheduler.add_job(
                self._execute_wrapper,
                'cron',
                args=[is_backfill, num_threads],
                day_of_week=scheduler_spec['day_of_week'],
                hour=scheduler_spec['hour'],
                minute=scheduler_spec['minute'],
                second=scheduler_spec['second'],
                max_instances=self._config['max_instances'],
                misfire_grace_time=scheduler_spec['misfire_grace_time']
            )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()

    def execute_now(self, is_backfill=False, num_threads=1):
        self._SYS_LOGGER.info("Entering execute now mode. Container will run now only once.")
        self._execute_wrapper(is_backfill=is_backfill, num_threads=num_threads)


class IntervalBatchContainer(DefaultBatchContainer):
    DATA_MODEL = DataModelType.BATCH

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

    def _execute_wrapper(self, is_backfill, num_threads):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
            node.unset_content()
        self.unset_counters()
        super().execute(is_backfill=is_backfill, num_threads=num_threads)

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
                max_instances=self._config['max_instances'],
                misfire_grace_time=scheduler_spec['misfire_grace_time']
            )
        background_scheduler.start()
        try:
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)
        except (KeyboardInterrupt, SystemExit):
            background_scheduler.shutdown()

    def execute_now(self, is_backfill=False, num_threads=1):
        self._SYS_LOGGER.info("Entering execute now mode. Container will run now only once.")
        self._execute_wrapper(is_backfill=is_backfill, num_threads=num_threads)


class NonStoppingBatchContainer(DefaultBatchContainer):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, container_name):
        super().__init__(container_name)

    def _execute_wrapper(self, num_threads):
        self.unset_status()
        for node in self.get_nodes():
            node.unset_status()
            node.unset_content()
        self.unset_counters()
        super().execute(is_backfill=False, num_threads=num_threads)

    def execute(self, is_backfill=False, num_threads=1, sleep_between_execution=TimeSleepObj.HALF_SECOND):
        while True:
            self._logger.info("Entering executing loop. Starting one execution...")
            self._execute_wrapper(num_threads=num_threads)
            if sleep_between_execution > 0:
                time.sleep(sleep_between_execution)
