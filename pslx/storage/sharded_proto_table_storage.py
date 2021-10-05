from collections import defaultdict
from pslx.core.exception import StorageReadException, StorageWriteException
from pslx.schema.enums_pb2 import StorageType
from pslx.schema.storage_pb2 import ProtoTableIndexMap
from pslx.storage.proto_table_storage import ProtoTableStorage
from pslx.storage.storage_base import StorageBase
from pslx.util.file_util import FileUtil
from pslx.util.proto_util import ProtoUtil


class ShardedProtoTableStorage(StorageBase):
    STORAGE_TYPE = StorageType.SHARDED_PROTO_TABLE_STORAGE

    def __init__(self, size_per_shard=100, logger=None):
        super().__init__(logger=logger)
        self._dir_name = None
        self._size_per_shard = size_per_shard

    def initialize_from_file(self, file_name):
        self._SYS_LOGGER.fatal("Initialize_from_file function is not implemented for storage type "
                               + ProtoUtil.get_name_by_value(enum_type=StorageType, value=self.STORAGE_TYPE) + '.')
        return

    def initialize_from_dir(self, dir_name):
        self._dir_name = dir_name
        self._index_map_file = FileUtil.join_paths_to_file(
            root_dir=dir_name,
            base_name='index_map.pb'
        )
        self._index_map_file = FileUtil.normalize_file_name(file_name=self._index_map_file)
        self._index_map = FileUtil.read_proto_from_file(
            proto_type=ProtoTableIndexMap,
            file_name=self._index_map_file
        )
        if self._index_map is None:
            self._index_map = ProtoTableIndexMap()
            self._index_map.cur_shard = 0
            assert self._size_per_shard > 0
            self._index_map.size_per_shard = self._size_per_shard
        else:
            if self._size_per_shard and self._index_map.size_per_shard != self._size_per_shard:
                self._logger.error("Please use the correct size per shard of [" + str(self._size_per_shard) + '].')
            self._size_per_shard = self._index_map.size_per_shard
            self._logger.info("Using size per shard of [" + str(self._size_per_shard) + '].')

    def is_empty(self):
        return self.get_num_shards() == 0

    def get_dir_name(self):
        return self._dir_name

    def get_num_shards(self):
        return self.get_latest_shard() + 1

    def get_latest_shard(self):
        return self._index_map.cur_shard

    def get_num_entries(self):
        return len(dict(self._index_map.index_map))

    def _shard_to_file(self, shard):
        return FileUtil.join_paths_to_file(
            root_dir=self._dir_name,
            base_name='data@' + str(shard) + '.pb'
        )

    def _file_to_shard(self, file):
        base_name = FileUtil.base_name(file)
        return int(base_name.replace('.pb', '').split('@')[1])

    def read_multiple(self, params):
        assert 'keys' in params

        related_shards = set()
        shard_to_key_map = defaultdict(list)
        for key in params['keys']:
            if key in self._index_map.index_map:
                related_shards.add(self._index_map.index_map[key])
                shard_to_key_map[self._index_map.index_map[key]].append(key)
        try:
            result = {}
            for shard in related_shards:
                related_proto_file = self._shard_to_file(shard=shard)
                proto_table = ProtoTableStorage()
                proto_table.initialize_from_file(file_name=related_proto_file)
                all_data = proto_table.read_all()
                for key in shard_to_key_map[shard]:
                    result[key] = all_data[key]

            return result
        except Exception as err:
            self._SYS_LOGGER.error("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            raise StorageReadException("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')

    def read(self, params):
        assert 'key' in params
        try:
            result = self.read_multiple(params={'keys': [params['key']]})
            value = list(result.values())[0]
            if 'message_type' in params:
                value = ProtoUtil.any_to_message(
                    any_message=value,
                    message_type=params['message_type']
                )

            return value
        except Exception as err:
            self._SYS_LOGGER.error("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Read dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            return None

    def read_all(self):
        num_shards = self.get_num_shards()
        result = {}
        try:
            for shard in range(num_shards):
                related_proto_file = self._shard_to_file(shard=shard)
                proto_table = ProtoTableStorage()
                proto_table.initialize_from_file(file_name=related_proto_file)
                all_data = proto_table.read_all()
                result.update(all_data)

            return result
        except Exception as err:
            self._SYS_LOGGER.error("Read all dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Read all dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            raise StorageReadException("Read all dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')

    def write(self, data, params=None):
        if not params:
            params = {}
        if 'overwrite' not in params:
            params['overwrite'] = True
        assert isinstance(data, dict)

        exising_shard_to_data_map = defaultdict(dict)
        new_data = {}
        for key, val in data.items():
            if key in self._index_map.index_map:
                exising_shard_to_data_map[self._index_map.index_map[key]][key] = val
            else:
                new_data[key] = val

        try:
            for shard, existing_data in exising_shard_to_data_map.items():
                related_proto_file = self._shard_to_file(shard=shard)
                proto_table = ProtoTableStorage()
                proto_table.initialize_from_file(file_name=related_proto_file)
                proto_table.write(data=existing_data, params=params)
            if new_data:
                all_new_keys = list(new_data.keys())
                latest_shard = self.get_latest_shard()
                latest_proto_file = self._shard_to_file(shard=latest_shard)
                proto_table = ProtoTableStorage()
                proto_table.initialize_from_file(file_name=latest_proto_file)
                latest_proto_table_size = proto_table.get_num_entries()
                proto_table.write(
                    data={key: new_data[key] for key in all_new_keys[:self._size_per_shard - latest_proto_table_size]},
                    params=params
                )
                for key in all_new_keys[:self._size_per_shard - latest_proto_table_size]:
                    self._index_map.index_map[key] = latest_shard

                if len(all_new_keys) > self._size_per_shard - latest_proto_table_size:
                    remaining_new_keys = all_new_keys[self._size_per_shard - latest_proto_table_size:]
                    start_index = 0
                    while start_index < len(remaining_new_keys):
                        data_for_new_shard = remaining_new_keys[start_index:start_index + self._size_per_shard]
                        latest_shard += 1
                        self._index_map.cur_shard += 1
                        proto_file = self._shard_to_file(shard=latest_shard)
                        self._logger.info("Write to new file with name [" + proto_file + '] and shard [' +
                                          str(latest_shard) + '].')
                        proto_table = ProtoTableStorage()
                        proto_table.initialize_from_file(file_name=proto_file)
                        proto_table.write(
                            data={key: new_data[key] for key in data_for_new_shard},
                            params=params
                        )
                        for key in data_for_new_shard:
                            self._index_map.index_map[key] = latest_shard

                        start_index += self._size_per_shard
                self._logger.info("Writing the index map to [" + self._index_map_file + '].')
                FileUtil.write_proto_to_file(
                    proto=self._index_map,
                    file_name=self._index_map_file
                )

        except Exception as err:
            self._SYS_LOGGER.error("Write to dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            self._logger.error("Write to dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')
            raise StorageWriteException("Write to dir [" + self.get_dir_name() + "] got exception: " + str(err) + '.')

    def resize_to_new_table(self, new_size_per_shard, new_dir_name):
        assert not FileUtil.does_dir_exist(dir_name=new_dir_name) or FileUtil.is_dir_empty(dir_name=new_dir_name)
        new_sptable_storage = ShardedProtoTableStorage(size_per_shard=new_size_per_shard)
        new_sptable_storage.initialize_from_dir(dir_name=new_dir_name)
        for shard in range(self.get_num_shards()):
            related_proto_file = self._shard_to_file(shard=shard)
            proto_table = ProtoTableStorage()
            proto_table.initialize_from_file(file_name=related_proto_file)
            new_sptable_storage.write(data=proto_table.read_all())
        return new_sptable_storage
