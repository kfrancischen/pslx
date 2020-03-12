from pslx.schema.common_pb2 import Credentials


class CommonUtil(object):
    @classmethod
    def make_email_credentials(cls, email_addr, password, email_server="smtp.gmail.com", email_server_port=25):
        credential = Credentials()
        credential.user_name = email_addr
        credential.password = password
        credential.others['email_server'] = email_server
        credential.others['email_server_port'] = str(email_server_port)
        return credential

    @classmethod
    def make_sql_server_credentials(cls, sql_host_ip, sql_port, user_name, password):
        credential = Credentials()
        credential.user_name = user_name
        credential.password = password
        credential.others['sql_host_ip'] = sql_host_ip
        credential.others['sql_port'] = sql_port
        return credential
