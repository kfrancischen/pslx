import argparse
from flask import Flask, render_template, request
from pslx.micro_service.container_backend.client import ContainerBackendClient
from pslx.micro_service.container_backend.rpc import ContainerBackendRPC
from pslx.schema.enums_pb2 import Status, ModeType, DataModelType
from pslx.schema.storage_pb2 import ContainerBackendValue
from pslx.storage.partitioner_storage import MinutelyPartitionerStorage
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.tool.logging_tool import LoggingTool
from pslx.tool.lru_cache_tool import LRUCacheTool
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil

CLIENT_NAME = 'PSLX_CONTAINER_BACKEND_UI'
container_backend_rpc_client_ui = Flask(
    __name__,
    template_folder='templates',
    static_folder='../../ui'
)
container_backend_rpc_client_ui.config.update(
    SECRET_KEY=CLIENT_NAME
)
parser = argparse.ArgumentParser()
parser.add_argument('--server_url', dest='server_url', default="", type=str, help='The server url.')
parser.add_argument('--root_certificate_path', dest='root_certificate_path', default="",
                    type=str, help='Path to the root certificate.')
args = parser.parse_args()
root_certificate = None
if args.root_certificate_path:
    with open(FileUtil.die_if_file_not_exist(file_name=args.root_certificate_path), 'r') as infile:
        root_certificate = infile.read()

container_backend_client = ContainerBackendClient(
    client_name=ContainerBackendClient.get_class_name() + '_FLASK_BACKEND',
    server_url=args.server_url,
    root_certificate=root_certificate
)

partitioner_lru_cache = LRUCacheTool(
    max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')
)
proto_table_lru_cache = LRUCacheTool(
    max_capacity=EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')
)
logger = LoggingTool(
    name=CLIENT_NAME,
    ttl=EnvUtil.get_pslx_env_variable(var='PSLX_INTERNAL_TTL')
)


def read_container_info():
    backend_folder = FileUtil.join_paths_to_dir(
        root_dir=EnvUtil.get_pslx_env_variable('PSLX_DATABASE'),
        base_name=ContainerBackendRPC.get_class_name()
    )
    backend_folder = FileUtil.create_dir_if_not_exist(dir_name=backend_folder)
    containers_info = {}
    for dir_to_data_model in FileUtil.list_dirs_in_dir(dir_name=backend_folder):
        for sub_dir in FileUtil.list_dirs_in_dir(dir_name=dir_to_data_model):
            sub_sub_dirs = FileUtil.list_dirs_in_dir(dir_name=sub_dir)
            dir_to_containers = ''
            for sub_sub_dir in sub_sub_dirs:
                if 'ttl' in sub_sub_dir:
                    dir_to_containers = sub_sub_dir
                else:
                    dir_to_containers = sub_dir
                break
            if dir_to_containers:
                for dir_name in FileUtil.list_dirs_in_dir(dir_name=dir_to_containers):
                    container_name = dir_name.strip('/').split('/')[-1]
                    partitioner_storage = partitioner_lru_cache.get(key=dir_name)
                    if not partitioner_storage:
                        partitioner_storage = MinutelyPartitionerStorage()
                        partitioner_lru_cache.set(key=dir_name, value=partitioner_storage)

                    partitioner_storage.initialize_from_dir(dir_name=dir_name)
                    latest_dir = partitioner_storage.get_latest_dir()
                    files = FileUtil.list_files_in_dir(dir_name=latest_dir)
                    if not files:
                        continue
                    proto_table_storage = proto_table_lru_cache.get(key=files[0])
                    if not proto_table_storage:
                        proto_table_storage = ProtoTableStorage()
                        proto_table_lru_cache.set(key=files[0], value=proto_table_storage)
                    proto_table_storage.initialize_from_file(file_name=files[0])
                    result_proto = proto_table_storage.read(
                        params={
                            'key': container_name,
                            'message_type': ContainerBackendValue,
                        }
                    )
                    container_info = {
                        'container_name': result_proto.container_name,
                        'status': ProtoUtil.get_name_by_value(
                                enum_type=Status, value=result_proto.container_status),
                        'updated_time': result_proto.updated_time,
                        'mode': ProtoUtil.get_name_by_value(enum_type=ModeType, value=result_proto.mode),
                        'data_model': ProtoUtil.get_name_by_value(
                            enum_type=DataModelType, value=result_proto.data_model),
                    }
                    operators_info = []
                    for key, val in dict(result_proto.operator_status_map).items():
                        operators_info.append({
                            'operator_name': key,
                            'status': ProtoUtil.get_name_by_value(
                                enum_type=Status, value=val),
                            'updated_time': result_proto.updated_time,
                        })
                    container_info['operators_info'] = operators_info
                    containers_info[(container_info['container_name'], container_info['mode'],
                                     container_info['data_model'])] = container_info

    return containers_info


@container_backend_rpc_client_ui.route("/", methods=['GET', 'POST'])
@container_backend_rpc_client_ui.route("/index.html", methods=['GET', 'POST'])
def index():
    containers_info = read_container_info()
    try:
        return render_template(
            'index.html',
            containers_info=sorted(containers_info.values(), key=lambda x: x['updated_time'], reverse=True)
        )
    except Exception as _:
        return render_template(
            'index.html',
            containers_info=[]
        )


@container_backend_rpc_client_ui.route('/view_container', methods=['GET', 'POST'])
def view_container():
    containers_info = read_container_info()
    container_name = request.args.get('container_name')
    mode = request.args.get('mode')
    data_model = request.args.get('data_model')
    try:
        return render_template(
            'view_container.html',
            container_name=container_name,
            operators_info=containers_info[(container_name, mode, data_model)]['operators_info']
        )
    except Exception as err:
        return render_template(
            'view_container.html',
            container_name=str(err),
            operators_info=[]
        )
