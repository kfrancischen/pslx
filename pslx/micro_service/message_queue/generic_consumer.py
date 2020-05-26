import base64
import os
import threading
import time
import pika
from pslx.core.base import Base
from pslx.core.exception import QueueAlreadyExistException, QueueConsumerNotInitializedException
from pslx.schema.rpc_pb2 import GenericRPCRequest
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimeSleepObj


class GenericQueueConsumer(Base):

    def __init__(self, consumer_name):
        super().__init__()
        self._consumer_name = consumer_name
        self._logger = LoggingTool(
            name=consumer_name,
            ttl=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_TTL')
        )
        self._connection_str = ''
        self._exchange = ''
        self._connection = None
        self._queue = None
        self._thread = None

        self._has_added_queue = False

    def get_consumer_name(self):
        return self._consumer_name

    def get_connection_str(self):
        return self._connection_str

    def get_queue_name(self):
        if self._queue:
            return self._queue.get_queue_name()
        else:
            return ''

    def create_consumer(self, exchange, connection_str):
        self.sys_log("Create consumer connection_str [" + connection_str + '] and exchange [' + exchange + '].')
        self._logger.info("Create consumer connection_str [" + connection_str + '] and exchange [' +
                          exchange + '].')
        self._connection_str = connection_str
        self._exchange = exchange
        self._connection = pika.SelectConnection(
            parameters=pika.URLParameters(connection_str),
            on_open_callback=self.on_open
        )

    def bind_queue(self, queue):
        if self._has_added_queue:
            self.sys_log("queue already exist, cannot bind any more.")
            self._logger.error("queue already exist, cannot bind any more.")
            raise QueueAlreadyExistException("queue already exist, cannot bind any more.")
        self.sys_log("Binding to queue with name [" + queue.get_queue_name() + '] to consumer [' +
                     self.get_consumer_name() + '].')
        self._logger.info("Binding to queue with name [" + queue.get_queue_name() + '] to consumer [' +
                          self.get_consumer_name() + '].')
        self._has_added_queue = True
        self._queue = queue

    def _process_message(self, ch, method, props, body):
        try:
            generic_request = ProtoUtil.string_to_message(
                message_type=GenericRPCRequest,
                string=base64.b64decode(body)
            )
            self._logger.info("Getting request with uuid [" + generic_request.uuid + '] in consumer ['
                              + self.get_consumer_name() + '].')
            response = self._queue.send_request(request=generic_request)
            response_str = ProtoUtil.message_to_string(proto_message=response)
            ch.basic_publish(exchange=self._exchange,
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=base64.b64encode(response_str))
        except Exception as err:
            self._logger.error("Consumer [" + self.get_consumer_name() + "] processing message with error: " +
                               str(err) + '.')

    def on_open(self, connection):
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        channel.exchange_declare(
            exchange=self._exchange,
            durable=True)
        channel.queue_delete(queue=self.get_queue_name())
        channel.queue_declare(queue=self.get_queue_name(), durable=True)
        channel.queue_bind(
            exchange=self._exchange,
            queue=self.get_queue_name(),
            routing_key=self.get_queue_name()
        )
        channel.basic_consume(
            queue=self.get_queue_name(),
            on_message_callback=self._process_message,
            auto_ack=True
        )

    def start_consumer(self):
        if not self._connection:
            raise QueueConsumerNotInitializedException("Queue not initialized for consumer [" +
                                                       self.get_consumer_name() + '].')

        self._thread = threading.Thread(
            target=self._connection.ioloop.start,
            name=self.get_consumer_name() + "_thread"
        )
        self._thread.daemon = True
        self._thread.start()

    def stop_consumer(self):
        if self._thread:
            self._thread.join()

        os._exit(1)


class GenericConsumer(Base):

    def __init__(self, connection_str):
        super().__init__()
        self._logger = LoggingTool(
            name=self.get_class_name(),
            ttl=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_TTL')
        )
        self._connection_str = connection_str
        self._queue_consumers = []

    def bind_queue(self, exchange, queue):
        queue_consumer = GenericQueueConsumer(consumer_name=queue.get_queue_name() + '_consumer')
        self._logger.info("Adding queue [" + queue.get_queue_name() + "] to consumer [" +
                          queue_consumer.get_consumer_name() + '].')
        queue_consumer.create_consumer(exchange=exchange, connection_str=self._connection_str)
        queue_consumer.bind_queue(queue=queue)
        self._queue_consumers.append(queue_consumer)

    def start_consumer(self):
        try:
            for consumer in self._queue_consumers:
                self._logger.info("Starting consumer [" + consumer.get_consumer_name() + '].')
                consumer.start_consumer()
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)

        except KeyboardInterrupt:
            for consumer in self._queue_consumers:
                consumer.stop_consumer()
