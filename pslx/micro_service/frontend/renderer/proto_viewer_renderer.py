from flask import render_template, request
from flask_login import login_required
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.micro_service.proto_viewer.client import ProtoViewerRPCClient
from pslx.util.file_util import FileUtil


client_map = {}

server_urls = []
for server_config in pslx_frontend_ui_app.config['frontend_config'].proto_viewer_config:
    url = server_config.server_url
    certificate_path = server_config.root_certificate_path
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
    server_urls.append(url)


@pslx_frontend_ui_app.route('/proto_viewer.html', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/view_proto', methods=['GET', 'POST'])
@login_required
def view_proto():
    all_urls = sorted(server_urls)
    if request.method == 'POST':
        try:
            server_url = request.form['server_url'].strip()
            proto_file_path = request.form['proto_file_path'].strip()
            message_type = request.form['message_type'].strip()
            module = request.form['module'].strip()
            pslx_frontend_logger.info("Select url " + server_url + ' and input path ' + proto_file_path + '.' +
                                      " with message type " + message_type + ' in module name ' + module + '.')
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
            all_urls.remove(server_url)
            all_urls = [server_url] + all_urls
            return render_template(
                'proto_viewer.html',
                proto_content=result_ui,
                server_urls=all_urls
            )
        except Exception as err:
            pslx_frontend_logger.error("Got error: " + str(err))
            return render_template(
                'proto_viewer.html',
                proto_content="Got error: " + str(err),
                server_urls=all_urls
            )
    else:
        return render_template(
            'proto_viewer.html',
            proto_content="",
            selected_server_url='',
            server_urls=server_urls
        )
