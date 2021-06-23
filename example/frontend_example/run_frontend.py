"""
PSLX_FRONTEND_CONFIG_PROTO_PATH=example/frontend_example/frontend_config.pb \
PYTHONPATH=. python example/frontend_example/run_frontend.py
"""
from pslx.micro_service.frontend import pslx_frontend_ui_app

if __name__ == "__main__":
    pslx_frontend_ui_app.run(host='0.0.0.0', port=4000, debug=True, use_reloader=False)
