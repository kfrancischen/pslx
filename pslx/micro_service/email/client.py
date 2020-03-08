from pslx.micro_service.rpc.client_base import ClientBase
from pslx.schema.rpc_pb2 import EmailPRCRequest


class EmailClient(ClientBase):

    def __init__(self, server_url):
        super().__init__(client_name=self.get_class_name(), server_url=server_url)

    def send_email(self, from_email, to_email, content, is_test=False):
        assert isinstance(content, str)
        request = EmailPRCRequest()
        request.is_test = is_test
        request.from_email = from_email
        request.to_email = to_email
        request.content = content
        self.send_request(request=request)
