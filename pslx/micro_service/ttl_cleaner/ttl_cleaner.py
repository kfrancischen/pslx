from pslx.batch.operator import BatchOperator
from pslx.batch.container import CronBatchContainer


class TTLCleanerOp(BatchOperator):
    def __init__(self, operator_name='ttl_cleaner'):
        super().__init__(operator_name=operator_name)

    def execute_impl(self):
        pass
