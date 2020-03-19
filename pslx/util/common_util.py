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
            config.sqlalchemy_database_path = dict_config['sqlalchemy_database_path']

            container_backend_config = FrontendConfig.ContainerBackendConfig()
            container_backend_config.server_url = dict_config['container_backend_config']['server_url']
            container_backend_config.root_certificate_path = \
                dict_config['container_backend_config']['root_certificate_path']
            config.container_backend_config.CopyFrom(container_backend_config)

            proto_viewer_config = FrontendConfig.ViewerConfig()
            for val in dict_config['proto_viewer_config'].values():
                proto_viewer_config.server_url_to_root_certificate_map[val['server_url']] = \
                    val['root_certificate_path']

            config.proto_viewer_config.CopyFrom(proto_viewer_config)

            file_viewer_config = FrontendConfig.ViewerConfig()
            for val in dict_config['file_viewer_config'].values():
                file_viewer_config.server_url_to_root_certificate_map[val['server_url']] = \
                    val['root_certificate_path']

            config.file_viewer_config.CopyFrom(file_viewer_config)

            credential = Credentials()
            credential.user_name = dict_config['user_name']
            credential.password = dict_config['password']
            config.credential.CopyFrom(credential)
            return config
