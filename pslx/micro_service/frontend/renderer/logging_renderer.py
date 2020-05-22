from flask import Response, request
from flask_login import login_required
import threading
import time
from pslx.micro_service.pubsub.subscriber import Subscriber
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_dedicated_logging_queue, pslx_frontend_logger
from pslx.schema.enums_pb2 import DiskLoggerLevel
from pslx.schema.rpc_pb2 import LoggingMessageRequest
from pslx.streaming.operator import StreamingOperator
from pslx.streaming.container import DefaultStreamingContainer
from pslx.util.color_util import ColorsUtil
from pslx.util.dummy_util import DummyUtil
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimeSleepObj


pslx_dedicated_subscriber = Subscriber(
    connection_str=pslx_frontend_ui_app.config['frontend_config'].logging_queue_config.connection_str
)
pslx_dedicated_subscriber.subscribe(
    exchange_name=pslx_frontend_ui_app.config['frontend_config'].logging_queue_config.exchange,
    topic_name=pslx_frontend_ui_app.config['frontend_config'].logging_queue_config.topic,
    message_type=LoggingMessageRequest
)


class FrontendDedicatedLoggingOp(StreamingOperator):

    def __init__(self):
        super().__init__(operator_name='FRONTEND_DEDICATED_LOGGING_OP')

    @staticmethod
    def pubsub_msg_parser(exchange_name, topic_name, message):
        pslx_dedicated_logging_queue.put(message)

    def execute_impl(self):
        pslx_dedicated_subscriber.bind_to_op(self)
        pslx_dedicated_subscriber.start()


class FrontendDedicatedLoggingContainer(DefaultStreamingContainer):
    def __init__(self):
        super().__init__(
            container_name='FRONTEND_DEDICATED_LOGGING_CONTAINER',
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )


def listening_to_log():
    dedicated_logging_op = FrontendDedicatedLoggingOp()
    dedicated_logging_container = FrontendDedicatedLoggingContainer()
    dedicated_logging_container.add_operator_edge(
        from_operator=dedicated_logging_op,
        to_operator=DummyUtil.dummy_streaming_operator()
    )
    dedicated_logging_container.initialize()
    dedicated_logging_container.execute()


dedicated_logging_thread = threading.Thread(target=listening_to_log)
dedicated_logging_thread.start()


pslx_dedicated_logging_list = []
strings_to_replace = [ColorsUtil.Foreground.GREEN, ColorsUtil.RESET, ColorsUtil.Foreground.YELLOW,
                      ColorsUtil.Foreground.RED]


def stream_template(template_name, **context):
    pslx_frontend_ui_app.update_template_context(context)
    t = pslx_frontend_ui_app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    return rv


@pslx_frontend_ui_app.route('/view_logging', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/realtime_logging.html', methods=['GET', 'POST'])
@login_required
def realtime_logging():
    try:
        key_words = request.form['key_words'].replace(' ', '').split(',')
    except Exception as _:
        key_words = []
    log_levels = request.form.getlist('log_level')
    pslx_frontend_logger.info('Key words changed to [' + ', '.join(key_words) +
                              '] and log levels changed to [' + ', '.join(log_levels) + '] for realtime logging.')

    def streaming_data_generator():
        for val in iter(pslx_dedicated_logging_queue.get, None):
            if ProtoUtil.get_name_by_value(enum_type=DiskLoggerLevel, value=val.level) in log_levels:
                message = val.message
                for string_to_replace in strings_to_replace:
                    message = message.replace(string_to_replace, '')
                contain_key_word = False if key_words else True
                for key_word in key_words:
                    if key_word in message:
                        contain_key_word = True
                        break

                if contain_key_word:
                    pslx_dedicated_logging_list.append(message)
                    if len(pslx_dedicated_logging_list) >= max(
                            int(EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_CACHE')), 700):
                        pslx_dedicated_logging_list.pop(0)

                    yield '\\n'.join([val for val in pslx_dedicated_logging_list])

            time.sleep(TimeSleepObj.ONE_TENTH_SECOND)
    return Response(stream_template('realtime_logging.html',
                                    logging_data=streaming_data_generator(),
                                    key_words=','.join(key_words),
                                    logging_levels={level: True for level in log_levels}))
