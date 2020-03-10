import json
import os
import requests

from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import InstantMessagingType, Status
from pslx.schema.rpc_pb2 import InstantMessagingRPCRequest
from pslx.tool.logging_tool import LoggingTool
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
        self._logger = LoggingTool(
            name=self.get_rpc_service_name(),
            ttl=os.getenv('PSLX_INTERNAL_TTL', 7)
        )

    def send_request_impl(self, request):
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
                self._logger.write_log("Slack failed to send message with err " + str(err))
                status = Status.FAILED
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
                self._logger.write_log("Rocketchat failed to send message with err " + str(err))
                status = Status.FAILED
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
                self._logger.write_log("Teams failed to send message with err " + str(err))
                status = Status.FAILED

        return None, status
