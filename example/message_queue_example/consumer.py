import requests

from pslx.micro_service.message_queue.queue_base import QueueBase
from pslx.micro_service.message_queue.generic_consumer import GenericConsumer
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import InstantMessagingRPCRequest
from pslx.storage.partitioner_storage import DailyPartitionerStorage
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
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

    partitioner_dir = FileUtil.join_paths_to_dir_with_mode(
        root_dir=FileUtil.join_paths_to_dir(
                    root_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DATABASE'),
                    base_name='msg_queue'
                ),
        base_name='msg_queue_example',
        ttl='1h'
    )
    storage = DailyPartitionerStorage()
    storage.initialize_from_dir(dir_name=partitioner_dir)

    slack_queue = SlackQueue(
        queue_name='slack_queue',
        queue_storage=storage
    )
    consumer.bind_queue(exchange='slack_exchange', queue=slack_queue)
    consumer.start_consumer()
