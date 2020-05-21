import smtplib

from pslx.micro_service.rpc.rpc_base import RPCBase
from pslx.schema.enums_pb2 import Status
from pslx.schema.rpc_pb2 import EmailPRCRequest
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil


class EmailRPC(RPCBase):
    REQUEST_MESSAGE_TYPE = EmailPRCRequest

    def __init__(self, rpc_storage):
        super().__init__(service_name=self.get_class_name(), rpc_storage=rpc_storage)

        self._logger = LoggingTool(
            name="PSLX_EMAIL_RPC",
            ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
        )
        self._credentials = {}
        self._email_servers = {}

    def _login(self, credentials):
        if not credentials.password:
            self._logger.error("Failed in logging to email [" + credentials.user_name + '].')
        else:
            self._credentials[credentials.user_name] = credentials
            email_server = smtplib.SMTP(
                credentials.others['email_server'],
                int(credentials.others['email_server_port'])
            )
            email_server.starttls()
            email_server.login(
                credentials.user_name,
                credentials.password
            )
            self._email_servers[credentials.user_name] = email_server
            self._logger.info("Successfully login to email [" + credentials.user_name + '].')

    def add_email_credentials(self, credentials):
        self._credentials[credentials.user_name] = credentials
        self._login(credentials)

    def get_response_and_status_impl(self, request):
        if request.from_email not in self._credentials:
            self._logger.error("Email address is not logged in at all.")
            return None, Status.FAILED

        def _send_email():
            if not request.is_test and request.to_email and request.content:
                self._email_servers[request.from_email].sendmail(
                    from_addr=request.from_email,
                    to_addrs=request.to_email,
                    msg=request.content
                )
        try:
            _send_email()
            self._logger.info("Succeeded in sending email directly to " + request.to_email + '.')
        except (smtplib.SMTPSenderRefused, smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError,
                smtplib.SMTPAuthenticationError) as err:
            self._logger.error("Sending email with exception: " + str(err) + '. Retry.')
            self._login(credentials=self._credentials[request.from_email])
            _send_email()
        return None, Status.SUCCEEDED
