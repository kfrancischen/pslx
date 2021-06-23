from flask import render_template, request
from flask_login import login_required
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.schema.storage_pb2 import ProtoTable
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


@pslx_frontend_ui_app.route('/proto_table_viewer.html', methods=['GET', 'POST'])
@pslx_frontend_ui_app.route('/view_proto_table', methods=['GET', 'POST'])
@login_required
def view_proto_table():
    if request.method == 'POST':
        try:
            proto_table_path = request.form['proto_table_path'].strip()
            value_type = request.form['value_type'].strip()
            modules = value_type.split('.')
            module, value_type = '.'.join(modules[:-1]), modules[-1]
            pslx_frontend_logger.info("Proto table viewer input path [" + proto_table_path + '] with value type [' +
                                      value_type + '] in module name [' + module + '].')
            result = FileUtil.read_proto_from_file(
                proto_type=ProtoTable, file_name=proto_table_path)
            value_type = ProtoUtil.infer_message_type_from_str(
                message_type_str=value_type,
                modules=module
            )
            proto_contents = []
            result_content = dict(result.data)
            for key in sorted(result_content.keys()):
                proto_val = ProtoUtil.any_to_message(
                    message_type=value_type,
                    any_message=result_content[key]
                )
                try:
                    proto_contents.append(
                        {
                            'key': key,
                            'val': ProtoUtil.message_to_text(proto_message=proto_val),
                        }
                    )
                except Exception as err:
                    pslx_frontend_logger.error("Proto table viewer Parsing proto with error " + str(err) + '.')
                    proto_contents.append(
                        {
                            'key': key,
                            'val': str(proto_val),
                        }
                    )

            return render_template(
                'proto_table_viewer.html',
                proto_contents=proto_contents
            )
        except Exception as err:
            pslx_frontend_logger.error("Got error rendering proto_table_viewer.html: " + str(err) + '.')
            return render_template(
                'proto_table_viewer.html',
                proto_contents=[]
            )
    else:
        return render_template(
            'proto_table_viewer.html',
            proto_contents=[]
        )
