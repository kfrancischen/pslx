from flask import render_template, request
from flask_login import login_required
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.micro_service.file_viewer.client import FileViewerRPCClient
from pslx.util.file_util import FileUtil


client_map = {}

file_viewer_config = \
    pslx_frontend_ui_app.config['frontend_config'].file_viewer_config.server_url_to_root_certificate_map
server_urls = []
for url, certificate_path in dict(file_viewer_config).items():
    pslx_frontend_logger.info("Getting url of " + url + " and certificate path " + certificate_path + '.')
    root_certificate = None
    if certificate_path:
        with open(FileUtil.die_if_file_not_exist(file_name=certificate_path), 'r') as infile:
            root_certificate = infile.read()

    file_viewer_client = FileViewerRPCClient(
        server_url=url
    )
    client_map[url] = {
        'client': file_viewer_client,
        'root_certificate': root_certificate,
    }
    server_urls.append(url)


@pslx_frontend_ui_app.route('/view_file', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/file_viewer.html', methods=['GET', 'POST'])
@login_required
def view_file():
    all_urls = sorted(server_urls)
    request_file_path = request.args.get('file_path')
    if request.method == 'POST' or request_file_path:
        try:
            server_url = request.form['server_url'].strip() if not request.args.get('server_url') else \
                request.args.get('server_url')
            file_path = request.form['file_path'].strip() if not request_file_path else request_file_path

            pslx_frontend_logger.info("Select url " + server_url + ' and input path ' + file_path + '.')
            result = client_map[server_url]['client'].view_file(
                file_path=file_path,
                root_certificate=client_map[server_url]['root_certificate']
            )
            all_urls.remove(server_url)
            all_urls = [server_url] + all_urls
            return render_template(
                'file_viewer.html',
                files_info=sorted(result['files_info'].values(), key=lambda item: item['file_path']),
                directories_info=sorted(result['directories_info']),
                file_path=file_path,
                selected_server_url=server_url,
                server_urls=all_urls
            )
        except Exception as err:
            pslx_frontend_logger.error("Got error: " + str(err))
            return render_template(
                'file_viewer.html',
                files_info={},
                directories_info=[],
                file_path='Got error: ' + str(err),
                selected_server_url='',
                server_urls=all_urls
            )
    elif request.method == 'GET':
        return render_template(
            'file_viewer.html',
            files_info={},
            directories_info=[],
            file_path='',
            selected_server_url='',
            server_urls=all_urls
        )
