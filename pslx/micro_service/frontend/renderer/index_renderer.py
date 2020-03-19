import datetime
from flask import render_template, request, abort, redirect, url_for, session
from flask_login import login_user, logout_user
from flask_login import LoginManager, login_required

from pslx.micro_service.frontend import pslx_frontend_ui_app
from pslx.micro_service.frontend.db.model import User


@pslx_frontend_ui_app.before_request
def make_session_permanent():
    session.permanent = True
    pslx_frontend_ui_app.permanent_session_lifetime = datetime.timedelta(minutes=20)
    session.modified = True


login_manager = LoginManager()
login_manager.init_app(pslx_frontend_ui_app)
login_manager.login_view = "login"


@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except Exception as _:
        return None


@pslx_frontend_ui_app.route('/login', methods=['POST', 'GET'])
def login():
    login_credential = pslx_frontend_ui_app.config['frontend_config'].credential

    if request.method == 'POST':
        if request.form['username'] == login_credential.user_name and \
                request.form['password'] == login_credential.password:
            user = User.query.filter_by(email=request.form['username']).first()
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
    return render_template("index.html")

