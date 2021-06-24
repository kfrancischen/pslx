PSLX also implements a message queue API, the application needs to inherit [queue_base.py](https://github.com/kfrancischen/pslx/blob/master/pslx/micro_service/message_queue/queue_base.py),
especially overriding the function of `get_response_and_status_impl` that takes a user defined request proto message to
a user defined response message. If the queue does not need response, please return None as the response.

An [example](https://github.com/kfrancischen/pslx/blob/master/example/message_queue_example/consumer.py) of this implementation of a queue for instant messaging including its consumer (like the RPC implementation) is
```python
import requests

from pslx.micro_service.message_queue.queue_base import QueueBase
from pslx.micro_service.message_queue.generic_consumer import GenericConsumer
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import InstantMessagingRPCRequest
from pslx.storage.partitioner_storage import DailyPartitionerStorage
from pslx.util.timezone_util import TimezoneUtil


class SlackQueue(QueueBase):
    REQUEST_MESSAGE_TYPE = InstantMessagingRPCRequest

    def get_response_and_status_impl(self, request):
        header = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Cache-Control': "no-cache",
        }
        slack_payload = "payload={'text':'" + request.message + "\nCurrent time is "\
                        + str(TimezoneUtil.cur_time_in_pst()) + "'}"
        status = Status.SUCCEEDED
        try:
            requests.post(
                request.webhook_url,
                data=slack_payload,
                headers=header
            )
        except Exception as err:
            self._logger.error("Slack failed to send message with err " + str(err))
            status = Status.FAILED
        return None, status


if __name__ == "__main__":
    consumer = GenericConsumer(
        connection_str='amqp://guest:guest@localhost:5672'
    )

    storage = DailyPartitionerStorage()
    storage.initialize_from_dir(dir_name="/galaxy/bb-d/pslx/test_data/storage2")

    slack_queue = SlackQueue(
        queue_name='slack_queue',
        queue_storage=storage
    )
    consumer.bind_queue(queue=slack_queue)
    consumer.start_consumer()
```

To use the message queue, one can also follow the [example code]() for an example of producer:

```python
from pslx.micro_service.message_queue.producer_base import ProducerBase
from pslx.schema.enums_pb2 import InstantMessagingType
from pslx.schema.rpc_pb2 import InstantMessagingRPCRequest


class SlackProducer(ProducerBase):

    def send_message(self, channel_name, webhook_url, message):
        request = InstantMessagingRPCRequest()
        request.is_test = False
        request.type = InstantMessagingType.SLACK
        request.channel_name = channel_name
        request.webhook_url = webhook_url
        request.message = message
        self.send_request(request=request)


if __name__ == "__main__":
    producer = SlackProducer(
        queue_name='slack_queue',
        connection_str='amqp://guest:guest@localhost:5672'
    )
    producer.send_message(
        channel_name='staging_test',
        webhook_url='https://hooks.slack.com/services/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',
        message="hello world"
    )
```

One note is that in order to use the message queue, please install [rabbitmq](https://www.rabbitmq.com/).