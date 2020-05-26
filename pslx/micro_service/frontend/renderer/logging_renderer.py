from flask import Response, request
from flask_login import login_required
import threading
import time
from pslx.micro_service.pubsub.subscriber import Subscriber
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_dedicated_logging_storage_path, pslx_frontend_logger
from pslx.schema.enums_pb2 import DiskLoggerLevel
from pslx.schema.rpc_pb2 import LoggingMessageRequest
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.streaming.operator import StreamingOperator
from pslx.streaming.container import DefaultStreamingContainer
from pslx.util.color_util import ColorsUtil
from pslx.util.dummy_util import DummyUtil
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil
from pslx.util.timezone_util import TimeSleepObj, TimezoneUtil


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
        self._pslx_dedicated_logging_storage = ProtoTableStorage(logger=pslx_frontend_logger)
        self._pslx_dedicated_logging_storage.initialize_from_file(
            file_name=pslx_dedicated_logging_storage_path
        )
        self._cached_logging = {}
        self._logging_storage_capacity = max(int(EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_CACHE')), 700)

    def pubsub_msg_parser(self, exchange_name, topic_name, message):
        self._cached_logging[str(TimezoneUtil.cur_time_in_pst())] = message
        if len(self._cached_logging) >= int(EnvUtil.get_pslx_env_variable('PSLX_RPC_FLUSH_RATE')):
            self._pslx_dedicated_logging_storage.write(
                data=self._cached_logging
            )
            self._cached_logging.clear()

            total_entries = self._pslx_dedicated_logging_storage.get_num_entries()
            if total_entries > self._logging_storage_capacity:
                try:
                    all_data = self._pslx_dedicated_logging_storage.read_all()
                    all_sorted_keys = sorted(list(all_data.keys()))
                    for i in range(total_entries - self._logging_storage_capacity):
                        key_to_delete = all_sorted_keys[i]
                        self._pslx_dedicated_logging_storage.delete(key=key_to_delete)
                except Exception as _:
                    pass

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

strings_to_replace = {
    ColorsUtil.Foreground.GREEN: '', 
    ColorsUtil.RESET: '', 
    ColorsUtil.Foreground.YELLOW: '',
    ColorsUtil.Foreground.RED: '', 
    '\n': '\\n',
    '\"': '',
}


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
        last_checked_key = None
        while True:
            pslx_dedicated_logging_storage = ProtoTableStorage(logger=pslx_frontend_logger)
            pslx_dedicated_logging_storage.initialize_from_file(
                file_name=pslx_dedicated_logging_storage_path
            )
            if pslx_dedicated_logging_storage.get_num_entries() == 0:
                time.sleep(TimeSleepObj.ONE_TENTH_SECOND)
                continue

            all_data = pslx_dedicated_logging_storage.read_all()
            all_sorted_keys = sorted(list(all_data.keys()))
            if all_sorted_keys[-1] == last_checked_key:
                time.sleep(TimeSleepObj.ONE_TENTH_SECOND)
                continue
            last_checked_key = all_sorted_keys[-1]

            pslx_dedicated_logging_list = []
            for key in all_sorted_keys:
                val = ProtoUtil.any_to_message(
                    message_type=LoggingMessageRequest,
                    any_message=all_data[key]
                )
                if ProtoUtil.get_name_by_value(enum_type=DiskLoggerLevel, value=val.level) in log_levels:
                    message = val.message
                    for string_to_replace, string_after_replacing in strings_to_replace.items():
                        message = message.replace(string_to_replace, string_after_replacing)
                    contain_key_word = False if key_words else True
                    for key_word in key_words:
                        if key_word in message:
                            contain_key_word = True
                            break

                    if contain_key_word:
                        pslx_dedicated_logging_list.append(message)
            yield '\\n'.join(pslx_dedicated_logging_list)

            time.sleep(TimeSleepObj.ONE_TENTH_SECOND)
    return Response(stream_template('realtime_logging.html',
                                    logging_data=streaming_data_generator(),
                                    key_words=','.join(key_words),
                                    logging_levels={level: True for level in log_levels}))
