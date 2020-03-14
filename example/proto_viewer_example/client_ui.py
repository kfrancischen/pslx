"""
PYTHONPATH=. python example/proto_viewer_example/client_ui.py \
--server_url_and_root_certificate_dict='{"localhost:11443": ""}'
"""
from pslx.micro_service.proto_viewer.client_ui import proto_viewer_rpc_client_ui

if __name__ == "__main__":
    proto_viewer_rpc_client_ui.run(host='0.0.0.0', port=5000, debug=True)
