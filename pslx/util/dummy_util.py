from pslx.core.operator_base import OperatorBase
from pslx.tool.logging_tool import LoggingTool
from pslx.schema.enums_pb2 import DataModelType


class DummyLogging(LoggingTool):
    def __init__(self, name=None, date=None, level=None, ttl=-0):
        super().__init__(name=None, date=date, level=level, ttl=ttl)

    def write_log(self, string):
        return


class DummyOperator(OperatorBase):
    def __init__(self, operator_name):
        super().__init__(operator_name=operator_name)

    def _execute(self, **kwargs):
        return True


class DummyUtil(object):

    @classmethod
    def dummy_logging(cls):
        return DummyLogging()

    @classmethod
    def dummy_operator(cls, operator_name='dummy_operator'):
        return DummyOperator(operator_name=operator_name)

    @classmethod
    def dummy_streaming_operator(cls, operator_name='dummy_streaming_operator'):
        op = DummyOperator(operator_name=operator_name)
        op.set_data_model(model=DataModelType.STREAMING)
        return op

    @classmethod
    def dummy_bach_operator(cls, operator_name='dummy_batch_operator'):
        op = DummyOperator(operator_name=operator_name)
        op.set_data_model(model=DataModelType.BATCH)
        return op
