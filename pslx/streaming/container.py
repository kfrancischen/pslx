from apscheduler.schedulers.background import BackgroundScheduler
import os

from pslx.core.container_base import ContainerBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.tool.logging_tool import LoggingTool
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimeZoneObj


class DefaultStreamingContainer(ContainerBase):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, tmp_file_folder='tmp/', ttl=-1):
        super().__init__(container_name, tmp_file_folder=tmp_file_folder, ttl=ttl)
        self._logger = LoggingTool(
            name=(self.get_class_name() + '__' +
                  ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + '__' + container_name),
            root_dir=os.getenv('DATA_ROOT_DIR', 'database/'),
            ttl=ttl
        )

    def execute(self, is_backfill=False, num_process=1):
        self.sys_log(string='Streaming Container does not allow multi-processing.')
        self._logger.write_log('Streaming Container does not allow multi-processing.')
        super().execute(is_backfill=is_backfill, num_process=1)


class ScheduledStreamingContainer(DefaultStreamingContainer):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, tmp_file_folder='tmp/', ttl=-1):
        super().__init__(container_name, tmp_file_folder=tmp_file_folder, ttl=ttl)
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

    def execute(self, is_backfill=False, num_process=1):
        background_scheduler = BackgroundScheduler(timezone=TimeZoneObj.WESTERN_TIMEZONE)
        background_scheduler.add_job(
            super().execute,
            'cron',
            args=[is_backfill, 1],
            day_of_week=self._scheduler_spec['day_of_week'],
            hour=self._scheduler_spec['hour'],
            minute=self._scheduler_spec['minute'],
            second=self._scheduler_spec['second']
        )
        background_scheduler.start()
