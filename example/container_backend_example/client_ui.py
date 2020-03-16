"""
PYTHONPATH=. python example/container_backend_example/client_ui.py \
--server_url=localhost:11443
"""
from pslx.micro_service.container_backend.client_ui import container_backend_rpc_client_ui

if __name__ == "__main__":
    container_backend_rpc_client_ui.run(host='localhost', port=5000, debug=True)
