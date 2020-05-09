import base64
import pika
from pslx.core.base import Base
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil


class Publisher(Base):

    def __init__(self, exchange_name, topic_name, connection_str):
        super().__init__()
        self._logger = LoggingTool(
            name=exchange_name + '_' + topic_name + '_publisher',
            ttl=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_TTL')
        )
        self._connection_str = connection_str
        self._topic_name = topic_name
        self._exchange_name = exchange_name
        self._connection = pika.BlockingConnection(
            pika.URLParameters(connection_str)
        )
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self._exchange_name,
            exchange_type='direct'
        )
        self._logger.info("Start publisher with topic name: " + self._topic_name + '.')

    def get_topic_name(self):
        return self._topic_name

    def get_exchange_name(self):
        return self._exchange_name

    def get_connection_str(self):
        return self._connection_str

    def publish(self, message):
        try:
            message_str = ProtoUtil.message_to_string(
                proto_message=message
            )
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._topic_name,
                body=base64.b64encode(message_str)
            )
        except Exception as err:
            self._logger.error("publish message failed with error " + str(err) + '.')
