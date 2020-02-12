from pslx.core.operator_base import OperatorBase
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import SortOrder


class StreamingOperator(OperatorBase):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, node_name, order=SortOrder.ORDER):
        super().__init__(node_name=node_name, order=order)

    def set_data_model(self, model):
        pass

    def unset_data_model(self):
        pass

    def convert_to_batch_operator(self):
        self.DATA_MODEL = DataModelType.BATCH

    def _execute(self):
        raise NotImplementedError
