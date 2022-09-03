from flask import Flask
from galaxy_py import glogging
from pslx.schema.common_pb2 import FrontendConfig
from pslx.util.env_util import EnvUtil
from pslx.util.proto_util import ProtoUtil

CLIENT_NAME = 'PSLX_FRONTEND_UI'
pslx_frontend_ui_app = Flask(__name__)

pslx_frontend_ui_app.config.update(
    SECRET_KEY=CLIENT_NAME
)
pslx_frontend_ui_app.config.update(
    SESSION_COOKIE_NAME=CLIENT_NAME + '_cookie'
)

pslx_frontend_logger = glogging.get_logger(
    log_name=CLIENT_NAME,
    log_dir=EnvUtil.get_pslx_env_variable(var='PSLX_DEFAULT_LOG_DIR') + 'PSLX_INTERNAL/frontend/'
)

frontend_config = EnvUtil.get_other_env_variable('PSLX_FRONTEND_CONFIG')

assert frontend_config != ''
pslx_frontend_ui_app.config['frontend_config'] = ProtoUtil.text_to_message(FrontendConfig, frontend_config)


pslx_frontend_ui_app.config['schemas'] = ['pslx.schema']

from pslx.micro_service.frontend.renderer import index_renderer, proto_viewer_renderer, \
    container_backend_renderer, proto_table_viewer_renderer
