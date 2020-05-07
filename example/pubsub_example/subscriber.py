from pslx.streaming.operator import StreamingOperator
from pslx.streaming.container import DefaultStreamingContainer
from pslx.micro_service.pubsub.subscriber import Subscriber
from pslx.schema.rpc_pb2 import HealthCheckerRequest
from pslx.util.dummy_util import DummyUtil


class SubscriberExampleOp(StreamingOperator):

    def __init__(self):
        super().__init__(operator_name='subscriber_example_op')

    @staticmethod
    def pubsub_msg_parser(exchange_name, topic_name, message):
        print(exchange_name, topic_name, message)

    def execute_impl(self):
        subscriber = Subscriber(
            connection_str='amqp://guest:guest@localhost:5672'
        )
        subscriber.bind_to_op(self)

        subscriber.subscribe(
            exchange_name='test_exchange_1',
            topic_name='test1',
            message_type=HealthCheckerRequest
        )
        subscriber.subscribe(
            exchange_name='test_exchange_2',
            topic_name='test2',
            message_type=HealthCheckerRequest
        )
        subscriber.start()


class SubscriberExampleContainer(DefaultStreamingContainer):
    def __init__(self):
        super().__init__(container_name='subscriber_example_container', ttl=7)


if __name__ == "__main__":
    op = SubscriberExampleOp()
    container = SubscriberExampleContainer()
    container.add_operator_edge(from_operator=op, to_operator=DummyUtil.dummy_streaming_operator())

    container.initialize()
    container.execute()
