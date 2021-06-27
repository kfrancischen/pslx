import json
import requests
from galaxy_py import glogging
from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import InstantMessagingType, Status
from pslx.schema.rpc_pb2 import InstantMessagingRPCRequest
from pslx.util.env_util import EnvUtil
from pslx.util.timezone_util import TimezoneUtil


class InstantMessagingRPC(RPCBase):
    REQUEST_MESSAGE_TYPE = InstantMessagingRPCRequest

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)
        self._type_to_sender_map = {
            InstantMessagingType.SLACK: self._send_by_slack,
            InstantMessagingType.ROCKETCHAT: self._send_by_rocketchat,
            InstantMessagingType.TEAMS: self._send_by_teams,
        }
        self._logger = glogging.get_logger(
            log_name='PSLX_INSTANT_MESSAGING_RPC',
            log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'PSLX_INTERNAL/im_rpc'
        )

    def get_response_and_status_impl(self, request):
        return self._type_to_sender_map[request.type](
            is_test=request.is_test,
            webhook_url=request.webhook_url,
            message=request.message
        )

    def _send_by_slack(self, is_test, webhook_url, message):
        header = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Cache-Control': "no-cache",
        }
        status = Status.SUCCEEDED
        if not is_test and webhook_url:
            slack_payload = "payload={'text':'" + message + "\nCurrent time is "\
                            + str(TimezoneUtil.cur_time_in_pst()) + "'}"
            try:
                requests.post(
                    webhook_url,
                    data=slack_payload,
                    headers=header
                )
            except Exception as err:
                self._logger.error("Slack failed to send message with err " + str(err))
                status = Status.FAILED
                error_payload = "payload={'text':'" + "Error: " + str(err) + "\nCurrent time is "\
                                + str(TimezoneUtil.cur_time_in_pst()) + "'}"
                requests.post(
                    webhook_url,
                    data=error_payload,
                    headers=header
                )
        return None, status

    def _send_by_rocketchat(self, is_test, webhook_url, message):
        status = Status.SUCCEEDED
        if not is_test and webhook_url:
            data = {
                "text": message + "\nCurrent time is " + str(TimezoneUtil.cur_time_in_pst()) +
                '\n-----------------------------------------------'
            }
            try:
                requests.post(webhook_url, json.dumps(data))
            except Exception as err:
                self._logger.error("Rocketchat failed to send message with err " + str(err))
                status = Status.FAILED
                error_data = {
                    "text": "Error: " + str(err) + "\nCurrent time is " + str(TimezoneUtil.cur_time_in_pst()) +
                    '\n-----------------------------------------------'
                }
                requests.post(webhook_url, json.dumps(error_data))
        return None, status

    def _send_by_teams(self, is_test, webhook_url, message):
        headers = {
            'Content-Type': "application/json"
        }
        status = Status.SUCCEEDED
        if not is_test and webhook_url:
            teams_json_data = {
                "text": message + ". Current time is " + str(TimezoneUtil.cur_time_in_pst()),
                "@content": "http://schema.org/extensions",
                "@type": "MessageCard",
            }
            try:
                requests.post(
                    webhook_url,
                    json=teams_json_data,
                    headers=headers
                )
            except Exception as err:
                self._logger.error("Teams failed to send message with err " + str(err))
                status = Status.FAILED
                error_json_data = {
                    "text": "Error: " + str(err) + ". Current time is " + str(TimezoneUtil.cur_time_in_pst()),
                    "@content": "http://schema.org/extensions",
                    "@type": "MessageCard",
                }
                requests.post(
                    webhook_url,
                    json=error_json_data,
                    headers=headers
                )

        return None, status
