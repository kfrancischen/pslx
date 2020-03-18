import argparse
import json
from flask import Flask, render_template, request
from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil

CLIENT_NAME = "PSLX_PROTO_VIEWER_UI"
proto_viewer_rpc_client_ui = Flask(
    __name__,
    template_folder='templates',
    static_folder='../../ui'
)
proto_viewer_rpc_client_ui.config.update(
    SECRET_KEY=CLIENT_NAME
)
parser = argparse.ArgumentParser()
parser.add_argument('--server_url_and_root_certificate_dict', dest='server_url_and_root_certificate_dict',
                    default="{}", type=json.loads,
                    help='json string containing url to root certificate dictionary.')
args = parser.parse_args()
url_and_certificate_dict = args.server_url_and_root_certificate_dict
client_map = {}
logger = LoggingTool(
    name=CLIENT_NAME,
    ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
)

for url, certificate_path in url_and_certificate_dict.items():
    logger.info("Getting url of " + url + " and certificate path " + certificate_path + '.')
    root_certificate = None
    if certificate_path:
        with open(FileUtil.die_if_file_not_exist(file_name=certificate_path), 'r') as infile:
            root_certificate = infile.read()

    proto_viewer_client = ProtoViewerRPCClient(
        server_url=url
    )
    client_map[url] = {
        'client': proto_viewer_client,
        'root_certificate': root_certificate,
    }


@proto_viewer_rpc_client_ui.route("/", methods=['GET', 'POST'])
@proto_viewer_rpc_client_ui.route("/index.html", methods=['GET', 'POST'])
def index():
    return render_template(
        'index.html',
        proto_content=''
    )


@proto_viewer_rpc_client_ui.route('/view_proto', methods=['GET', 'POST'])
def view_proto():
    if request.method == 'POST':
        try:
            server_url = request.form['server_url']
            proto_file_path = request.form['proto_file_path']
            message_type = request.form['message_type']
            module = request.form['module']
            result = client_map[server_url]['client'].view_proto(
                proto_file_path=proto_file_path,
                message_type=message_type,
                module=module,
                root_certificate=client_map[server_url]['root_certificate']
            )
            result_ui = ''
            for key, val in result.items():
                if key == 'proto_content':
                    result_ui += key + ':\n\n' + val + '\n\n'
                else:
                    result_ui += key + ': ' + val + '\n\n'
            return render_template(
                'index.html',
                proto_content=result_ui
            )
        except Exception as err:
            logger.error("Got error: " + str(err))
            return render_template(
                'index.html',
                proto_content="Got error: " + str(err)
            )
