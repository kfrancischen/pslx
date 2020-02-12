import os

from pslx.core.container_base import ContainerBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.tool.logging_tool import LoggingTool
from pslx.util.proto_util import ProtoUtil


class StreamingContainer(ContainerBase):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, container_name, tmp_file_folder='tmp/', ttl=-1):
        super().__init__(container_name, tmp_file_folder=tmp_file_folder, ttl=ttl)
        self._logger = LoggingTool(
            name=(self.get_class_name() + '-' +
                  ProtoUtil.get_name_by_value(enum_type=DataModelType, value=self.DATA_MODEL) + container_name),
            root_dir=os.getenv('DATA_ROOT_DIR', 'database/'),
            ttl=ttl
        )

    def execute(self, is_backfill=False, num_process=1):
        self.log_print(string='Streaming Container does not allow multi-processing.')
        self._logger.write_log('Streaming Container does not allow multi-processing.')
        super().execute(is_backfill=is_backfill, num_process=1)
