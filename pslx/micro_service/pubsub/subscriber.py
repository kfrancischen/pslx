import base64
import pika
from pslx.core.base import Base
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil


class Subscriber(Base):

    def __init__(self, connection_str, logger=DummyUtil.dummy_logger()):
        super().__init__()
        self._logger = logger
        self._connection_str = connection_str
        self._connection = pika.BlockingConnection(
            pika.URLParameters(connection_str)
        )
        self._channel = self._connection.channel()
        tmp_result = self._channel.queue_declare(queue='', exclusive=True)
        self._tmp_queue_name = tmp_result.method.queue
        self._topic_names_to_types = {}
        self._op = None

    def subscribe(self, exchange_name, topic_name, message_type):
        # declare tmp queue
        self._SYS_LOGGER.info("Please make sure topic name is in the exchange.")
        try:
            self._channel.queue_bind(
                exchange=exchange_name,
                queue=self._tmp_queue_name,
                routing_key=topic_name
            )
            self._logger.info("Succeeded in subscribing to topic [" + topic_name + '] in exchange [' +
                              exchange_name + '].')
            self._topic_names_to_types[exchange_name + ':' + topic_name] = message_type
        except Exception as err:
            self._logger.info("Failed to subscribe to topic [" + topic_name + '] in exchange [' + exchange_name +
                              '] with error ' + str(err) + '.')

    def bind_to_op(self, op):
        self._op = op

    def parse_message(self, exchange_name, topic_name, message):
        if not self._op:
            self._logger.error("Please bind to operator to parse message.")
        else:
            self._op.pubsub_msg_parser(exchange_name=exchange_name, topic_name=topic_name, message=message)

    def on_response(self, ch, method, properties, body):
        try:
            message_type = self._topic_names_to_types[method.exchange + ':' + method.routing_key]
            message = ProtoUtil.string_to_message(
                message_type=message_type,
                string=base64.b64decode(body)
            )
            return self.parse_message(
                exchange_name=method.exchange,
                topic_name=method.routing_key,
                message=message
            )
        except Exception as err:
            self._logger.error("Getting response with error " + str(err) + '.')

    def get_topic_names(self):
        return ', '.join(self._topic_names_to_types.keys())

    def start(self):
        self._channel.basic_consume(
            queue=self._tmp_queue_name,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self._channel.start_consuming()
