from pslx.core.operator_base import OperatorBase
from pslx.tool.logging_tool import LoggingTool
from pslx.schema.enums_pb2 import DataModelType


class DummyLogging(LoggingTool):
    def __init__(self, name=None, date=None, root_dir=None, level=None, ttl=-0):
        super().__init__(name=None, date=date, root_dir=root_dir, level=level, ttl=ttl)

    def write_log(self, string):
        return


class DummyOperator(OperatorBase):
    def __init__(self, node_name):
        super().__init__(node_name=node_name)

    def _execute(self, **kwargs):
        return True


class DummyUtil(object):

    @classmethod
    def dummy_logging(cls):
        return DummyLogging()

    @classmethod
    def dummy_operator(cls, node_name='dummy_operator'):
        return DummyOperator(node_name=node_name)

    @classmethod
    def dummy_streaming_operator(cls, node_name='dummy_streaming_operator'):
        op = DummyOperator(node_name=node_name)
        op.set_data_model(model=DataModelType.STREAMING)
        return op

    @classmethod
    def dummy_bach_operator(cls, node_name='dummy_batch_operator'):
        op = DummyOperator(node_name=node_name)
        op.set_data_model(model=DataModelType.BATCH)
        return op
