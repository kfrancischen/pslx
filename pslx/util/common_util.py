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
    def make_mongodb_server_credentials(cls, mongodb_host_ip, mongodb_port, user_name, password):
        credential = Credentials()
        credential.user_name = user_name
        credential.password = password
        credential.others['mongodb_host_ip'] = mongodb_host_ip
        credential.others['mongodb_port'] = mongodb_port
        return credential

    @classmethod
    def make_frontend_config(cls, yaml_path):
        if not FileUtil.does_file_exist(file_name=yaml_path):
            return FrontendConfig()
        else:
            dict_config = YamlUtil.yaml_to_dict(file_name=yaml_path)
            config = FrontendConfig()

            container_backend_config = FrontendConfig.ServerConfig()
            container_backend_config.server_url = dict_config['CONTAINER_BACKEND_CONFIG']['SERVER_URL']
            config.container_backend_config.CopyFrom(container_backend_config)


            if 'INSTANT_MESSAGING_CONFIG' in dict_config:
                for val in dict_config['INSTANT_MESSAGING_CONFIG'].values():
                    server_config = config.instant_messaging_config.add()
                    server_config.server_url = val['SERVER_URL']

            if 'EMAIL_CONFIG' in dict_config:
                for val in dict_config['EMAIL_CONFIG'].values():
                    server_config = config.email_config.add()
                    server_config.server_url = val['SERVER_URL']

            credential = Credentials()
            credential.user_name = dict_config['USER_NAME']
            credential.password = dict_config['PASSWORD']
            config.credential.CopyFrom(credential)
            return config
