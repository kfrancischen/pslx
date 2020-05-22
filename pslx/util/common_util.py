from pslx.schema.common_pb2 import Credentials, FrontendConfig
from pslx.util.file_util import FileUtil
from pslx.util.yaml_util import YamlUtil


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

    @classmethod
    def make_frontend_config(cls, yaml_path):
        if not FileUtil.does_file_exist(file_name=yaml_path):
            return FrontendConfig()
        else:
            dict_config = YamlUtil.yaml_to_dict(file_name=yaml_path)
            config = FrontendConfig()
            config.sqlalchemy_database_path = dict_config['SQLALCHEMY_DATABASE_PATH']

            container_backend_config = FrontendConfig.ServerConfig()
            container_backend_config.server_url = dict_config['CONTAINER_BACKEND_CONFIG']['SERVER_URL']
            container_backend_config.root_certificate_path = \
                dict_config['CONTAINER_BACKEND_CONFIG']['ROOT_CERTIFICATE_PATH']
            config.container_backend_config.CopyFrom(container_backend_config)

            if 'PROTO_VIEWER_CONFIG' in dict_config:
                for val in dict_config['PROTO_VIEWER_CONFIG'].values():
                    server_config = config.proto_viewer_config.add()
                    server_config.server_url = val['SERVER_URL']
                    server_config.root_certificate_path = val['ROOT_CERTIFICATE_PATH']

            if 'FILE_VIEWER_CONFIG' in dict_config:
                for val in dict_config['FILE_VIEWER_CONFIG'].values():
                    server_config = config.file_viewer_config.add()
                    server_config.server_url = val['SERVER_URL']
                    server_config.root_certificate_path = val['ROOT_CERTIFICATE_PATH']

            if 'INSTANT_MESSAGING_CONFIG' in dict_config:
                for val in dict_config['INSTANT_MESSAGING_CONFIG'].values():
                    server_config = config.instant_messaging_config.add()
                    server_config.server_url = val['SERVER_URL']
                    server_config.root_certificate_path = val['ROOT_CERTIFICATE_PATH']

            if 'RPC_IO_CONFIG' in dict_config:
                for val in dict_config['RPC_IO_CONFIG'].values():
                    server_config = config.rpc_io_config.add()
                    server_config.server_url = val['SERVER_URL']
                    server_config.root_certificate_path = val['ROOT_CERTIFICATE_PATH']

            if 'EMAIL_CONFIG' in dict_config:
                for val in dict_config['EMAIL_CONFIG'].values():
                    server_config = config.email_config.add()
                    server_config.server_url = val['SERVER_URL']
                    server_config.root_certificate_path = val['ROOT_CERTIFICATE_PATH']

            msg_queue_config = FrontendConfig.MessageQueueConfig()
            msg_queue_config.exchange = dict_config['LOGGING_QUEUE_CONFIG']['EXCHANGE']
            msg_queue_config.topic = dict_config['LOGGING_QUEUE_CONFIG']['TOPIC']
            msg_queue_config.connection_str = dict_config['LOGGING_QUEUE_CONFIG']['CONNECTION_STR']
            config.logging_queue_config.CopyFrom(msg_queue_config)

            credential = Credentials()
            credential.user_name = dict_config['USER_NAME']
            credential.password = dict_config['PASSWORD']
            config.credential.CopyFrom(credential)
            return config
