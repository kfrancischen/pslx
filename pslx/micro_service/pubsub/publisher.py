import base64
import threading
import pika
from pslx.core.base import Base
from pslx.util.dummy_util import DummyUtil
from pslx.util.proto_util import ProtoUtil


class Publisher(Base):

    def __init__(self, exchange_name, topic_name, connection_str, logger=DummyUtil.dummy_logging()):
        super().__init__()
        self._logger = logger
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
        self._logger.info("Start publisher with topic name [" + self._topic_name + '] in exchange [' +
                          self._exchange_name + '].')
        self._emit_lock = threading.Lock()

    def get_topic_name(self):
        return self._topic_name

    def get_exchange_name(self):
        return self._exchange_name

    def get_connection_str(self):
        return self._connection_str

    def publish(self, message):
        self._emit_lock.acquire()
        try:
            message_str = ProtoUtil.message_to_string(
                proto_message=message
            )
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._topic_name,
                body=base64.b64encode(message_str),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            self._logger.info("Succeeded in publishing the data to exchange [" + self._exchange_name +
                              "] with topic name [" + self._topic_name + '].')
        except Exception as err:
            self._logger.error("publish message failed with error " + str(err) + '.')
        finally:
            self._emit_lock.release()
