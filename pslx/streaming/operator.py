from pslx.core.operator_base import OperatorBase
from pslx.schema.enums_pb2 import DataModelType


class StreamingOperator(OperatorBase):
    DATA_MODEL = DataModelType.STREAMING

    def __init__(self, operator_name):
        super().__init__(operator_name=operator_name)
        self._config = {
            'save_snapshot': False,
            'allow_container_snapshot': False,
            'allow_failure': True,
        }

    def set_data_model(self, model):
        pass

    def unset_data_model(self):
        pass

    def convert_to_batch_operator(self):
        self.DATA_MODEL = DataModelType.BATCH
        self.set_config(config={
            'allow_container_snapshot': True,
            'allow_failure': False,
        })

    def execute_impl(self):
        raise NotImplementedError
