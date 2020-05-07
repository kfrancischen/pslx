PSLX implements the basic Pubsub model through Rabbitmq. The publisher is directly usable as all the basic building blocks are already implemented.
In addition, here the message that goes through the pubsub model needs to be a proto message. Here is an example of publisher:

```python
import time
from pslx.micro_service.pubsub.publisher import Publisher
from pslx.util.timezone_util import TimeSleepObj
from pslx.schema.rpc_pb2 import HealthCheckerRequest

if __name__ == "__main__":
    publisher1 = Publisher(
        exchange_name='test_exchange_1',
        topic_name='test1',
        connection_str='amqp://guest:guest@localhost:5672'
    )

    message1 = HealthCheckerRequest()
    message1.server_url = 'test1'
    message1.secure = False

    publisher2 = Publisher(
        exchange_name='test_exchange_2',
        topic_name='test2',
        connection_str='amqp://guest:guest@localhost:5672'
    )

    message2 = HealthCheckerRequest()
    message2.server_url = 'test2'
    message2.secure = True

    while True:
        print("Publish message...")
        publisher1.publish(message=message1)
        publisher2.publish(message=message2)
        time.sleep(TimeSleepObj.ONE_SECOND)
```

For subscriber, the user needs to wrap it in an operator with the following function defined:
```python
pubsub_parse_message(exchange_name, topic_name, message)
```
which takes in the exchange and topic (routing_key) of the message. The purpose of this function to allow user to have the access of handling the message. 

To bind the subscriber to the operator, one can use:
```python
bind_to_op(op)
```
An example subscriber of the above publisher is
```python
from pslx.streaming.operator import StreamingOperator
from pslx.streaming.container import DefaultStreamingContainer
from pslx.micro_service.pubsub.subscriber_base import Subscriber
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
    container.add_operator_edge(
        from_operator=op,
        to_operator=DummyUtil.dummy_streaming_operator(operator_name='dummy_streaming_operator')
    )

    container.initialize()
    container.execute()
```