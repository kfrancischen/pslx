"""
PYTHONPATH=. python example/file_viewer_example/client_ui.py \
--server_url_and_root_certificate_dict='{"localhost:11443": ""}'
"""
from pslx.micro_service.file_viewer.client_ui import file_viewer_rpc_client_ui

if __name__ == "__main__":
    file_viewer_rpc_client_ui.run(host='0.0.0.0', port=5000, debug=True)
