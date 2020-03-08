import os

from pslx.micro_service.email.rpc import EmailRPC
from pslx.micro_service.rpc.generic_server import GenericServer
from pslx.util.common_util import CommonUtil

if __name__ == "__main__":
    server_url = "localhost:11443"
    example_rpc = EmailRPC(request_storage=None)
    credentials = CommonUtil.make_email_credentials(
        email_addr='alphahunter2019@gmail.com',
        password=os.getenv('PSLX_EMAIL_PWD', '')
    )
    example_rpc.add_email_credentials(credentials=credentials)
    example_server = GenericServer(server_name='example')
    example_server.create_server(max_worker=1, server_url=server_url)
    example_server.bind_rpc(rpc=example_rpc)
    example_server.start_server()
