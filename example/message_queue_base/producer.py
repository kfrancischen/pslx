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
        exchange='slack_exchange',
        queue_name='slack_queue',
        connection_str='amqp://guest:guest@localhost:5672'
    )
    producer.send_message(
        channel_name='staging_test',
        webhook_url='https://hooks.slack.com/services/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',
        message="hello world"
    )

