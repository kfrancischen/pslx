from flask import render_template, request, abort, redirect, url_for
from flask_login import login_user, logout_user
from flask_login import LoginManager, login_required

from pslx.schema.enums_pb2 import Status
from pslx.micro_service.frontend import pslx_frontend_ui_app, pslx_frontend_logger
from pslx.micro_service.frontend.model.model import User
from pslx.util.proto_util import ProtoUtil
from pslx.util.rpc_util import RPCUtil


login_manager = LoginManager()
login_manager.init_app(pslx_frontend_ui_app)
login_manager.login_view = "login"


@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except Exception as err:
        pslx_frontend_logger.error("Load user with error message: " + str(err) + '.')
        return None


@pslx_frontend_ui_app.route('/login', methods=['POST', 'GET'])
def login():
    login_credential = pslx_frontend_ui_app.config['frontend_config'].credential

    if request.method == 'POST':
        pslx_frontend_logger.info('Index logging in with ' + str(dict(request.form)) + " from ip [" +
                                  request.remote_addr + '].')
        if request.form['username'] == login_credential.user_name and \
                request.form['password'] == login_credential.password:
            user = User.query.filter_by(username=request.form['username']).first()
            if user:
                assert user.password == request.form['password']
                login_user(user)
            else:
                new_user = User(username=request.form['username'], password=request.form['password'])
                new_user.save()
                login_user(new_user)
            return redirect(request.args.get('next'))
        else:
            abort(401)
    else:
        return render_template('login.html')


@pslx_frontend_ui_app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))


@pslx_frontend_ui_app.route("/", methods=['GET', 'POST'])
@pslx_frontend_ui_app.route("/index.html", methods=['GET', 'POST'])
@login_required
def index():
    config = pslx_frontend_ui_app.config['frontend_config']
    service_info = []
    if config.container_backend_config.server_url:
        server, port = config.container_backend_config.server_url.split(':')
        pslx_frontend_logger.info("Index checking health for url [" + config.container_backend_config.server_url + '].')
        status, qps = RPCUtil.check_health_and_qps(
            server_url=config.container_backend_config.server_url,
            root_certificate_path=config.container_backend_config.root_certificate_path
        )
        service_info.append({
            'name': 'container_backend',
            'server': server,
            'port': port,
            'status': ProtoUtil.get_name_by_value(enum_type=Status, value=status),
            'qps': round(qps, 3),
        })
    for server_config in pslx_frontend_ui_app.config['frontend_config'].proto_viewer_config:
        server, port = server_config.server_url.split(':')
        status, qps = RPCUtil.check_health_and_qps(
            server_url=server_config.server_url,
            root_certificate_path=server_config.root_certificate_path
        )
        pslx_frontend_logger.info("Index checking health for url [" + server_config.server_url + '].')
        service_info.append({
            'name': 'proto_viewer',
            'server': server,
            'port': port,
            'status': ProtoUtil.get_name_by_value(enum_type=Status, value=status),
            'qps': round(qps, 3),
        })

    for server_config in pslx_frontend_ui_app.config['frontend_config'].file_viewer_config:
        server, port = server_config.server_url.split(':')
        status, qps = RPCUtil.check_health_and_qps(
            server_url=server_config.server_url,
            root_certificate_path=server_config.root_certificate_path
        )
        pslx_frontend_logger.info("Index checking health for url [" + server_config.server_url + '].')
        service_info.append({
            'name': 'file_viewer',
            'server': server,
            'port': port,
            'status': ProtoUtil.get_name_by_value(enum_type=Status, value=status),
            'qps': round(qps, 3),
        })

    for server_config in pslx_frontend_ui_app.config['frontend_config'].instant_messaging_config:
        server, port = server_config.server_url.split(':')
        status, qps = RPCUtil.check_health_and_qps(
            server_url=server_config.server_url,
            root_certificate_path=server_config.root_certificate_path
        )
        pslx_frontend_logger.info("Index checking health for url [" + server_config.server_url + '].')
        service_info.append({
            'name': 'instant_messaging',
            'server': server,
            'port': port,
            'status': ProtoUtil.get_name_by_value(enum_type=Status, value=status),
            'qps': round(qps, 3),
        })

    for server_config in pslx_frontend_ui_app.config['frontend_config'].rpc_io_config:
        server, port = server_config.server_url.split(':')
        status, qps = RPCUtil.check_health_and_qps(
            server_url=server_config.server_url,
            root_certificate_path=server_config.root_certificate_path
        )
        pslx_frontend_logger.info("Index checking health for url [" + server_config.server_url + '].')
        service_info.append({
            'name': 'rpc_io',
            'server': server,
            'port': port,
            'status': ProtoUtil.get_name_by_value(enum_type=Status, value=status),
            'qps': round(qps, 3),
        })

    for server_config in pslx_frontend_ui_app.config['frontend_config'].email_config:
        server, port = server_config.server_url.split(':')
        status, qps = RPCUtil.check_health_and_qps(
            server_url=server_config.server_url,
            root_certificate_path=server_config.root_certificate_path
        )
        pslx_frontend_logger.info("Index checking health for url [" + server_config.server_url + '].')
        service_info.append({
            'name': 'email',
            'server': server,
            'port': port,
            'status': ProtoUtil.get_name_by_value(enum_type=Status, value=status),
            'qps': round(qps, 3),
        })

    return render_template("index.html",
                           service_info=sorted(service_info, key=lambda x: x['name']))
