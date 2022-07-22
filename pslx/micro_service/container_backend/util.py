from pslx.schema.storage_pb2 import ContainerBackendValue
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.util.env_util import EnvUtil
from pslx.util.file_util import FileUtil
from pslx.util.timezone_util import TimezoneUtil


class ContainerBackendUtil(object):

    def get_response_impl(backend_folder, request, max_num_snapshot=-1, lru_cache=None):
        storage_value = ContainerBackendValue()
        storage_value.container_name = request.container_name
        storage_value.container_status = request.status
        for operator_name, operator_snapshot in dict(request.operator_snapshot_map).items():
            operator_info = ContainerBackendValue.OperatorInfo()
            operator_info.status = operator_snapshot.status
            for parent in operator_snapshot.node_snapshot.parents_names:
                operator_info.parents.append(parent)

            operator_info.start_time = operator_snapshot.start_time
            operator_info.end_time = operator_snapshot.end_time
            operator_info.log_file = operator_snapshot.log_file
            storage_value.operator_info_map[operator_name].CopyFrom(operator_info)

        storage_value.mode = request.mode
        storage_value.data_model = request.data_model
        storage_value.updated_time = str(TimezoneUtil.cur_time_in_pst())
        storage_value.start_time = request.start_time
        storage_value.end_time = request.end_time
        storage_value.log_file = request.log_file
        storage_value.run_cell = request.run_cell
        storage_value.snapshot_cell = request.snapshot_cell
        for key in request.counters:
            storage_value.counters[key] = request.counters[key]
        storage_value.ttl = int(EnvUtil.get_pslx_env_variable('PSLX_BACKEND_CONTAINER_TTL'))

        storage = lru_cache.get(key=storage_value.container_name) if lru_cache else None

        if not storage:
            storage = ProtoTableStorage()
            storage.initialize_from_file(
                file_name=FileUtil.join_paths_to_file(
                    root_dir=backend_folder,
                    base_name=storage_value.container_name + '.pb'
                )
            )
            if lru_cache:
                lru_cache.set(
                    key=backend_folder,
                    value=storage
                )
        all_data = storage.read_all()
        if len(all_data) >= int(EnvUtil.get_pslx_env_variable('PSLX_INTERNAL_CACHE')) > 0:
            key_to_delete = sorted(all_data.keys())[0]
            storage.delete(key=key_to_delete)

        storage.write(
            data={storage_value.start_time: storage_value}
        )

        if max_num_snapshot > 0:
            all_files = sorted(FileUtil.list_files_in_dir(backend_folder))
            for file_name in all_files[:-max_num_snapshot]:
                FileUtil.remove_file(file_name)
