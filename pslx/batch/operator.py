from pslx.core.operator_base import OperatorBase
from pslx.schema.enums_pb2 import DataModelType


class BatchOperator(OperatorBase):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, operator_name):
        super().__init__(operator_name=operator_name)
        self._config = {
            'save_snapshot': True,
            'allow_container_snapshot': True,
            'allow_failure': False,
        }

    def set_data_model(self, model):
        pass

    def unset_data_model(self):
        pass

    def convert_to_streaming_operator(self):
        self.DATA_MODEL = DataModelType.STREAMING
        self.set_config(config={
            'allow_container_snapshot': False,
            'allow_failure': True,
        })

    def execute_impl(self):
        raise NotImplementedError
