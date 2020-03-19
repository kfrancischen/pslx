from flask import render_template, request
from flask_login import login_required
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient
from pslx.util.file_util import FileUtil


client_map = {}

proto_viewer_config = \
    pslx_frontend_ui_app.config['frontend_config'].proto_viewer_config.server_url_to_root_certificate_map
for url, certificate_path in dict(proto_viewer_config).items():
    pslx_frontend_logger.info("Getting url of " + url + " and certificate path " + certificate_path + '.')
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


@pslx_frontend_ui_app.route('/proto_viewer.html', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/view_proto', methods=['GET', 'POST'])
@login_required
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
                'proto_viewer.html',
                proto_content=result_ui
            )
        except Exception as err:
            pslx_frontend_logger.error("Got error: " + str(err))
            return render_template(
                'proto_viewer.html',
                proto_content="Got error: " + str(err)
            )
    else:
        return render_template(
            'proto_viewer.html',
            proto_content=""
        )
