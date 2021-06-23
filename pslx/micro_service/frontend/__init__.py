from flask import Flask
from galaxy_py import glogging
from pslx.schema.common_pb2 import FrontendConfig
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil

CLIENT_NAME = 'PSLX_FRONTEND_UI'
pslx_frontend_ui_app = Flask(__name__)

pslx_frontend_ui_app.config.update(
    SECRET_KEY=CLIENT_NAME
)
pslx_frontend_logger = glogging.get_logger(
    log_name=CLIENT_NAME,
    log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'PSLX_FRONTEND/'
)

frontend_config_file = EnvUtil.get_pslx_env_variable('PSLX_FRONTEND_CONFIG_PROTO_PATH')

assert frontend_config_file != ''
pslx_frontend_ui_app.config['frontend_config'] = FileUtil.read_proto_from_file(
    proto_type=FrontendConfig,
    file_name=frontend_config_file
)

pslx_partitioner_lru_cache = LRUCacheTool(
    max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')
)
pslx_proto_table_lru_cache = LRUCacheTool(
    max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')
)

from pslx.micro_service.frontend.renderer import index_renderer, proto_viewer_renderer, \
    container_backend_renderer, proto_table_viewer_renderer
