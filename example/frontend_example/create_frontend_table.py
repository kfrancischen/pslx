"""
 PSLX_FRONTEND_CONFIG_PROTO_PATH=example/frontend_example/frontend_config.pb \
 PYTHONPATH=. python example/frontend_example/create_frontend_table.py
"""
from pslx.micro_service.frontend import pslx_frontend_db

if __name__ == '__main__':
    pslx_frontend_db.create_all()
