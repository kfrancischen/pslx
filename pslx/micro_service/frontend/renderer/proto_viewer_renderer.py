from flask import render_template, request
from flask_login import login_required
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


@pslx_frontend_ui_app.route('/proto_viewer.html', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/view_proto', methods=['GET', 'POST'])
@login_required
def view_proto():
    if request.method == 'POST':
        try:
            proto_file_path = request.form['proto_file_path'].strip()
            message = request.form['message'].strip()
            modules = message.split('.')
            module, message_type_str = '.'.join(modules[:-1]), modules[-1]
            pslx_frontend_logger.info("Proto viewer input path [" + proto_file_path + '] with message type [' +
                                      message_type_str + '] in module name [' + module + '].')
            message_type = ProtoUtil.infer_message_type_from_str(
                message_type_str=message_type_str,
                modules=module
            )
            proto_message = FileUtil.read_proto_from_file(
                proto_type=message_type, file_name=proto_file_path)

            result_ui = ProtoUtil.message_to_text(proto_message=proto_message)

            return render_template(
                'proto_viewer.html',
                proto_content=result_ui
            )
        except Exception as err:
            pslx_frontend_logger.error("Got error rendering proto_viewer.html: " + str(err) + '.')
            return render_template(
                'proto_viewer.html',
                proto_content="Got error rendering proto_viewer.html: " + str(err) + '.'
            )
    else:
        return render_template(
            'proto_viewer.html',
            proto_content=""
        )
