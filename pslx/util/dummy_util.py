from pslx.core.operator_base import OperatorBase
from pslx.schema.enums_pb2 import DataModelType


class DummyOperator(OperatorBase):
    def __init__(self, operator_name):
        super().__init__(operator_name=operator_name)

    def execute_impl(self):
        pass


class DummyLogger(object):
    def __init__(self, log_name, log_dir):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def fatal(self, *args, **kwargs):
        pass

    def critical(self, *args, **kwargs):
        pass


class DummyUtil(object):

    @classmethod
    def dummy_logger(cls):
        return DummyLogger("", "")

    @classmethod
    def dummy_operator(cls, operator_name='dummy_operator'):
        return DummyOperator(operator_name=operator_name)

    @classmethod
    def dummy_streaming_operator(cls, operator_name='dummy_streaming_operator'):
        op = DummyOperator(operator_name=operator_name)
        op.set_data_model(model=DataModelType.STREAMING)
        return op

    @classmethod
    def dummy_batch_operator(cls, operator_name='dummy_batch_operator'):
        op = DummyOperator(operator_name=operator_name)
        op.set_data_model(model=DataModelType.BATCH)
        return op
