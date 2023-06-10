from pslx.core.operator_base import OperatorBase
from pslx.micro_service.pubsub.publisher import Publisher
from pslx.micro_service.pubsub.subscriber import Subscriber
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.common_pb2 import BatchOperatorFinishMessage
from pslx.util.dummy_util import DummyLogger
from pslx.util.rpc_util import EndpointUtil
from pslx.util.timezone_util import TimezoneUtil


class BatchOperator(OperatorBase):
    DATA_MODEL = DataModelType.BATCH

    def __init__(self, operator_name, logger=DummyLogger()):
        super().__init__(operator_name=operator_name, logger=logger)
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


class BatchFinishOperator(BatchOperator):
    def __init__(self, operator_name, endpoint, connection_str, message):
        super().__init__(operator_name=operator_name)
        self._exchange_name, self._topic_name = EndpointUtil.get_exchange_info(endpoint)
        self._message = message
        try:
            self._publisher = Publisher(
                exchange_name=self._exchange_name,
                topic_name=self._topic_name,
                connection_str=connection_str
            )
        except Exception as _:
            self._publisher = None

    def update_message(self, message):
        self._message = message

    def execute_impl(self):
        if self._publisher:
            message = BatchOperatorFinishMessage()
            message.message = self._message
            message.end_time = str(TimezoneUtil.cur_time_in_pst())
            message.exchange_name = self._exchange_name
            message.topic_name = self._topic_name
            message.operator_name = self.get_node_name()
            self._publisher.publish(message=message)


class BatchStartOperator(BatchOperator):

    def __init__(self, operator_name, endpoint, connection_str):
        super().__init__(operator_name=operator_name)
        self._exchange_name, self._topic_name = EndpointUtil.get_exchange_info(endpoint)
        try:
            self._subscriber = Subscriber(
                connection_str=connection_str
            )
            self._subscriber.bind_to_op(self)
            self._subscriber.subscribe(
                exchange_name=self._exchange_name,
                topic_name=self._topic_name,
                message_type=BatchOperatorFinishMessage
            )
        except Exception as _:
            self._subscriber = None

    def execute_impl(self):
        self._subscriber.start()

    @classmethod
    def start_criteria(message):
        return True

    def pubsub_msg_parser(self, exchange_name, topic_name, message):
        if self.start_criteria(message):
            self._subscriber.close()
