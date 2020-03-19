from pslx.micro_service.frontend import pslx_frontend_db
from flask_login import UserMixin


class User(UserMixin, pslx_frontend_db.Model):
    id = pslx_frontend_db.Column(pslx_frontend_db.Integer, primary_key=True)
    username = pslx_frontend_db.Column(pslx_frontend_db.String(200), unique=True, nullable=False)
    password = pslx_frontend_db.Column('password', pslx_frontend_db.String(120), nullable=False)

    def save(self):
        pslx_frontend_db.session.add(self)
        pslx_frontend_db.session.commit()

    def delete(self):
        pslx_frontend_db.session.delete(self)
        pslx_frontend_db.session.commit()

