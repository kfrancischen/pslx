import argparse
import json
from flask import Flask, render_template, request
from pslx.micro_service.file_viewer.client import FileViewerRPCClient
from pslx.tool.logging_tool import LoggingTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil

CLIENT_NAME = 'PSLX_FILE_VIEWER_UI'
file_viewer_rpc_client_ui = Flask(
    __name__,
    template_folder='templates',
    static_folder='../../ui'
)
file_viewer_rpc_client_ui.config.update(
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
    logger.write_log("Getting url of " + url + " and certificate path " + certificate_path + '.')
    root_certificate = None
    if certificate_path:
        with open(FileUtil.die_if_file_not_exist(file_name=certificate_path), 'r') as infile:
            root_certificate = infile.read()

    proto_viewer_client = FileViewerRPCClient(
        server_url=url
    )
    client_map[url] = {
        'client': proto_viewer_client,
        'root_certificate': root_certificate,
    }


@file_viewer_rpc_client_ui.route("/", methods=['GET', 'POST'])
@file_viewer_rpc_client_ui.route("/index.html", methods=['GET', 'POST'])
def index():
    return render_template(
        'index.html',
        files_info={},
        directories_info=[],
        file_path='',
        server_url=''
    )


@file_viewer_rpc_client_ui.route('/view_file', methods=['GET', 'POST'])
def view_file():
    request_file_path = request.args.get('file_path')
    server_url = request.args.get('server_url')
    if request.method == 'POST' or request_file_path:
        try:
            server_url = request.form['server_url'] if not server_url else server_url
            file_path = request.form['file_path'] if not request_file_path else request_file_path
            result = client_map[server_url]['client'].view_file(
                file_path=file_path,
                root_certificate=client_map[server_url]['root_certificate']
            )
            return render_template(
                'index.html',
                files_info=sorted(result['files_info'].values(), key=lambda item: item['file_path']),
                directories_info=sorted(result['directories_info']),
                file_path=file_path,
                server_url=server_url
            )
        except Exception as err:
            logger.write_log("Got error: " + str(err))
            return render_template(
                'index.html',
                files_info={},
                directories_info=[],
                file_path='Got error: ' + str(err),
                server_url=server_url
            )
