from pslx.core.base import Base
from pslx.micro_service.rpc_io.client import PartitionerStorageRPC
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


class LocalPartitionerFetcher(Base):
    MESSAGE_TYPE = None

    def __init__(self, partitioner):
        super().__init__()
        self._partitioner = partitioner

    def fetch_latest(self):
        latest_dir = self._partitioner.get_latest_dir()
        proto_table = ProtoTableStorage()
        proto_table.initialize_from_file(
            file_name=FileUtil.join_paths_to_file(
                root_dir=latest_dir,
                base_name='data.pb'
            )
        )
        all_data = proto_table.read_all()
        if all_data:
            max_key = max(all_data.keys())
            return ProtoUtil.any_to_message(
                message_type=self.MESSAGE_TYPE,
                any_message=all_data[max_key]
            )
        else:
            return None

    def fetch_oldest(self):
        oldest_dir = self._partitioner.get_oldest_dir_in_root_directory()
        proto_table = ProtoTableStorage()
        proto_table.initialize_from_file(
            file_name=FileUtil.join_paths_to_file(
                root_dir=oldest_dir,
                base_name='data.pb'
            )
        )
        all_data = proto_table.read_all()
        if all_data:
            min_key = min(all_data.keys())
            return ProtoUtil.any_to_message(
                message_type=self.MESSAGE_TYPE,
                any_message=all_data[min_key]
            )
        else:
            return None

    def fetch_range(self, start_time, end_time):
        data = self._partitioner.read_range(
            params={
                'start_time': start_time,
                'end_time': end_time,
            }
        )
        start_time, end_time = str(start_time.replace(tzinfo=None)), str(end_time.replace(tzinfo=None))
        data_content = {}
        for val in data.values():
            data_content.update(val)
        result = {}

        for key in sorted(data_content.keys()):
            if key < start_time or key > end_time:
                continue
            else:
                item = ProtoUtil.any_to_message(
                    message_type=self.MESSAGE_TYPE,
                    any_message=data_content[key]
                )
                result[key] = item
        return result


class RemotePartitionerFetcher(Base):
    MESSAGE_TYPE = None
    PARTITIONER_STORAGE_TYPE = None

    def __init__(self, partitioner_dir, server_url, root_certificate=None):
        super().__init__()
        self._partitioner_dir = partitioner_dir
        self._server_url = server_url
        self._root_certificate = root_certificate
        self._rpc_client = PartitionerStorageRPC(
            client_name="remote_partitioner_fetcher",
            server_url=server_url
        )

    def fetch_latest(self):
        all_data = self._rpc_client.read(
            file_or_dir_path=self._partitioner_dir,
            params={
                'PartitionerStorageType': self.PARTITIONER_STORAGE_TYPE,
                'is_proto_table': True,
            },
            root_certificate=self._root_certificate
        )
        if all_data:
            max_key = max(all_data.keys())
            return ProtoUtil.any_to_message(
                message_type=self.MESSAGE_TYPE,
                any_message=all_data[max_key]
            )
        else:
            return None

    def fetch_oldest(self):
        all_data = self._rpc_client.read(
            file_or_dir_path=self._partitioner_dir,
            params={
                'PartitionerStorageType': self.PARTITIONER_STORAGE_TYPE,
                'is_proto_table': True,
                'read_oldest': True,
            },
            root_certificate=self._root_certificate
        )
        if all_data:
            min_key = min(all_data.keys())
            return ProtoUtil.any_to_message(
                message_type=self.MESSAGE_TYPE,
                any_message=all_data[min_key]
            )
        else:
            return None

    def fetch_range(self, start_time, end_time):
        data = self._rpc_client.read_range(
            file_or_dir_path=self._partitioner_dir,
            params={
                'PartitionerStorageType': self.PARTITIONER_STORAGE_TYPE,
                'start_time': start_time,
                'end_time': end_time,
                'is_proto_table': True,
            },
            root_certificate=self._root_certificate
        )
        if not data:
            return {}
        start_time, end_time = str(start_time.replace(tzinfo=None)), str(end_time.replace(tzinfo=None))
        data_content = {}
        for val in data.values():
            data_content.update(val)
        result = {}

        for key in sorted(data_content.keys()):
            if key < start_time or key > end_time:
                continue
            else:
                item = ProtoUtil.any_to_message(
                    message_type=self.MESSAGE_TYPE,
                    any_message=data_content[key]
                )
                result[key] = item
        return result
