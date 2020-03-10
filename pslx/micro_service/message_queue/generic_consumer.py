import os
import pika
import threading
import time
from pslx.core.base import Base
from pslx.core.exception import QueueAlreadyExistException, QueueConsumerNotInitializedException
from pslx.tool.logging_tool import LoggingTool
from pslx.util.timezone_util import TimeSleepObj


class GenericQueueConsumer(Base):

    def __init__(self, consumer_name):
        super().__init__()
        self._consumer_name = consumer_name
        self._logger = LoggingTool(
            name=self.get_queue_name(),
            ttl=os.getenv('PSLX_INTERNAL_TTL', 7)
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
        self.sys_log("Create consumer connection_str = " + connection_str + ' and exchange = ' + exchange + '.')
        self._logger.write_log("Create consumer connection_str = " + connection_str + ' and exchange = ' +
                               exchange + '.')
        self._connection_str = connection_str
        self._exchange = exchange
        self._connection = pika.SelectConnection(
            parameters=pika.URLParameters(connection_str),
            on_open_callback=self.on_open
        )

    def bind_queue(self, queue):
        if self._has_added_queue:
            self.sys_log("queue already exist, cannot bind any more.")
            self._logger.write_log("queue already exist, cannot bind any more.")
            raise QueueAlreadyExistException
        self.sys_log("Binding to queue with name " + queue.get_queue_name() + '.')
        self._has_added_queue = True
        self._queue = queue

    def _process_message(self, ch, method, props, body):
        try:
            self._logger.write_log("Getting request with uuid " + body.uuid)
            response = self._queue.send_request(request=body)

            ch.basic_publish(exchange=self._exchange,
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=response)
        except Exception as err:
            self._logger.write_log("Error: " + str(err))

    def on_open(self, connection):
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        channel.exchange_declare(
            exchange=self._exchange,
            durable=True)

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
            raise QueueConsumerNotInitializedException

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
            ttl=os.getenv('PSLX_INTERNAL_TTL', 7)
        )
        self._connection_str = connection_str
        self._queue_consumers = []

    def bind_queue(self, exchange, queue):
        queue_consumer = GenericQueueConsumer(consumer_name=queue.get_queue_name() + '_consumer')
        self._logger.write_log("Adding queue of " + queue.get_queue_name() + " to consumer " +
                               queue_consumer.get_consumer_name() + '.')
        queue_consumer.create_consumer(exchange=exchange, connection_str=self._connection_str)
        queue_consumer.bind_queue(queue=queue)
        self._queue_consumers.append(queue_consumer)

    def start_consumer(self):
        try:
            for consumer in self._queue_consumers:
                self._logger.write_log("Starting consumer " + consumer.get_consumer_name() + '.')
                consumer.start_consumer()
            while True:
                time.sleep(TimeSleepObj.ONE_SECOND)

        except KeyboardInterrupt:
            for consumer in self._queue_consumers:
                consumer.stop_consumer()

