from flask import render_template, request
from flask_login import login_required
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient
from pslx.util.file_util import FileUtil


client_map = {}

file_viewer_config = \
    pslx_frontend_ui_app.config['frontend_config'].file_viewer_config.server_url_to_root_certificate_map
for url, certificate_path in dict(file_viewer_config).items():
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


@pslx_frontend_ui_app.route('/view_file', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/file_viewer.html', methods=['GET', 'POST'])
@login_required
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
            pslx_frontend_logger.error("Got error: " + str(err))
            return render_template(
                'file_viewer.html',
                files_info={},
                directories_info=[],
                file_path='Got error: ' + str(err),
                server_url=server_url
            )
    if request.method == 'GET':
        return render_template(
            'file_viewer.html',
            files_info={},
            directories_info=[],
            file_path='',
            server_url=server_url
        )
