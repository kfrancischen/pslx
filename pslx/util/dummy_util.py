from pslx.core.operator_base import OperatorBase
from pslx.tool.logging_tool import LoggingTool
from pslx.schema.enums_pb2 import OperatorStatus


class DummyLoggingTool(LoggingTool):
    def __init__(self, name=None, date=None, root_dir=None, level=None,retention=0):
        super().__init__(name=None, date=date, root_dir=root_dir, level=level, retention=retention)

    def write_log(self, string):
        return


class DummyOperator(OperatorBase):
    def __init__(self, node_name):
        super().__init__(node_name=node_name)

    def _execute(self, **kwargs):
        self.set_status(status=OperatorStatus.SUCCESSFUL)
        return
