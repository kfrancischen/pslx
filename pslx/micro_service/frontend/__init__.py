from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from pslx.schema.common_pb2 import FrontendConfig
from pslx.tool.logging_tool import LoggingTool
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil

CLIENT_NAME = 'PSLX_FRONTEND_UI'
pslx_frontend_ui_app = Flask(__name__)

pslx_frontend_ui_app.config.update(
    SECRET_KEY=CLIENT_NAME
)
pslx_frontend_logger = LoggingTool(
    name=CLIENT_NAME,
    ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
)

frontend_config_file = EnvUtil.get_pslx_env_variable('PSLX_FRONTEND_CONFIG_PROTO_PATH')

assert frontend_config_file != ''
pslx_frontend_ui_app.config['frontend_config'] = FileUtil.read_proto_from_file(
    proto_type=FrontendConfig,
    file_name=frontend_config_file
)

pslx_frontend_ui_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
pslx_frontend_ui_app.config['SQLALCHEMY_DATABASE_URI'] =\
    'sqlite:///' + pslx_frontend_ui_app.config['frontend_config'].sqlalchemy_database_path
pslx_frontend_logger.info("sqlalchemy database uri " +
                          str(pslx_frontend_ui_app.config['SQLALCHEMY_DATABASE_URI']) + '.')

pslx_frontend_db = SQLAlchemy(pslx_frontend_ui_app)

pslx_partitioner_lru_cache = LRUCacheTool(
    max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')
)
pslx_proto_table_lru_cache = LRUCacheTool(
    max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')
)

pslx_dedicated_logging_storage_path = FileUtil.join_paths_to_dir_with_mode(
    root_dir=EnvUtil.get_pslx_env_variable('PSLX_DATABASE') + '/PSLX_DEDICATED_LOGGING',
    base_name='dedicated_logging.pb'
)

from pslx.micro_service.frontend.renderer import index_renderer, file_viewer_renderer, proto_viewer_renderer, \
    container_backend_renderer, proto_table_viewer_renderer, logging_renderer
