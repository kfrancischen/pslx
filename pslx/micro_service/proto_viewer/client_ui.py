import argparse
from flask import Flask, render_template, request
from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient

proto_viewer_rpc_client_ui = Flask(
    __name__,
    template_folder='templates',
    static_folder='../../ui'
)
proto_viewer_rpc_client_ui.config.update(
    SECRET_KEY='PSLX_PROTO_VIEWER_UI'
)
parser = argparse.ArgumentParser()
parser.add_argument('--server_url', dest='server_url', default="", type=str,
                    help='URL to the server.')
args = parser.parse_args()
proto_viewer_client = ProtoViewerRPCClient(
    server_url=args.server_url
)


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
            proto_file_path = request.form['proto_file_path']
            message_type = request.form['message_type']
            module = request.form['module']
            result = proto_viewer_client.view_proto(
                proto_file_path=proto_file_path,
                message_type=message_type,
                module=module
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
        except Exception as _:
            pass
