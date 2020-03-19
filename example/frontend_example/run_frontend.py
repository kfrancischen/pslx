"""
PSLX_FRONTEND_CONFIG_PROTO_PATH=example/frontend_example/frontend_config.pb \
PYTHONPATH=. python example/frontend_example/run_frontend.py
"""
from pslx.micro_service.frontend import pslx_frontend_ui_app

if __name__ == "__main__":
    pslx_frontend_ui_app.run(host='localhost', port=5001, debug=True)
