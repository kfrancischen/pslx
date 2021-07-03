PSLX supports in total five different types of storage: `StorageType.DEFAULT_STORAGE`, `StorageType.FIXED_SIZE_STORAGE`,
`StorageType.PROTO_TABLE_STORAGE`, `StorageType.SHARDED_PROTO_TABLE_STORAGE` and `StorageType.PARTITIONER_STORAGE`, and the `StorageType.PARTITIONER_STORAGE` also support
five different types of timestamp based partitions: `PartitionerStorageType.MINUTELY`, `PartitionerStorageType.HOURLY`, `PartitionerStorageType.DAILY`,
`PartitionerStorageType.MONTHLY`, `PartitionerStorageType.YEARLY`. The related enums are defined in [schema](schema.md), and their implementations are
in the [storage](https://github.com/kfrancischen/pslx/tree/master/pslx/storage) folder.

The four stage all inherit from a parent class and [`storage_base.py`](https://github.com/kfrancischen/pslx/blob/master/pslx/storage/storage_base.py), where
each inheritance needs to implement its own read and write functions. Besides these two functions, there are a few functions
that are shard across all storage types.

```python
__init__(logger=None)
```
* Description: Construct a storage.
* Arguments:
    1. logger: the logger. Default value is None.

```python
set_config(config)
```
* Description: Updates the initial config.
* Arguments:
    1. config: the config that is added to the existing config.

```python
get_storage_type()
```
* Description: Get the storage type of the storage.
* Return: the storage type of the storage, one of `StorageType.DEFAULT_STORAGE`, `StorageType.FIXED_SIZE_STORAGE`,
`StorageType.PROTO_TABLE_STORAGE` and `StorageType.PARTITIONER_STORAGE`.

```python
initialize_from_file(file_name)
```
* Description: initialize the storage from a file, only supported by `StorageType.DEFAULT_STORAGE`, `StorageType.FIXED_SIZE_STORAGE`,
and `StorageType.PROTO_TABLE_STORAGE`.
* Arguments:
    1. file_name: the file that is used to initialize the storage.

```python
initialize_from_dir(dir_name)
```
* Description: initialize the storage from a directory, only supported by `StorageType.PARTITIONER_STORAGE`.
* Arguments:
    1. dir_name: the directory name that is used to initialize the storage.


### Default Storage Documentation

```python
read(params)
```
* Description: Read from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. The underlying file needs to be a text file, and the default storage supports read/write from both top and down (see
    the definition of `ReadRuleType` and `WriteRuleType` in [schema](schema.md)), and can be set by function `set_config(config)`
    by overwriting string `read_rule_type` and `write_rule_type` with the correct enum. The default values for them are `ReadRuleType.READ_FROM_BEGINNING` and
    `WriteRuleType.WRITE_FROM_END`.
    2. The params in the `read(params)` function supports the number of lines to read from the file. The way to set this field is
    to pass `num_line` to the param (a dictionary). If the `num_line` exceeds the total number of lines in the file, an error will be raised.
    3. After reading the underlying file, the file handler will move accordingly. For example, after reading one line from the file, the second time
    the storage will start by reading the second line of the file. The can be reset by calling `start_from_first_line()`.

```python
write(data, params)
```
* Description: Write to the storage.
* Arguments:
    1. data: the write parameters.
* Explanation:
    1. If the data is a string, it will be written to the underlying file from top or bottom depending on the value of `write_rule_type`.
    2. If the data is a list, it will be joined with `delimter` set in the params. If key `delimiter` is not present in params, comma will be used by default.

```python
start_from_first_line()
```
* Description: Reset the reader to read from the first line (from top or bottom).

### Proto Table Storage

```python
read(params)
```
* Description: Read from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. proto table is a key-value storage, and therefore the params need to contain field of `key`.
    2. The value of the proto table is a proto message, and if the field of `message_type` is provided,
     the reader will correctly deserialize to the desired protobuf. Otherwise it will only an `Any` type message.
    3. Under PSLX convention, the underlying file name needs to end with `.pb`.

```python
read_multiple(params)
```
* Description: Read multiple key from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. proto table is a key-value storage, and therefore the params need to contain field of `keys`, which is a list.


```python
read_all()
```
* Description: Read all the data.
* Return: the key, value dictionary of the table, with value being the `Any` proto format.

```python
write(data, params)
```
* Description: Write to the storage.
* Arguments:
    1. data: the write parameters.
* Explanation:
    1. The data needs to be a dictionary of `key, value` with key being a string and value being a protobuf message of any user defined
    types.
    2. The params can contain `overwrite` with its value a boolean indicating whether to overwrite the value if the key already
    exists in the proto table.

```python
delete(key)
```
* Description: Delete key and the corresponding entry from the proto table.
* Arguments:
    1. key: the key of the entry to be deleted.

```python
delete_multiple(keys)
```
* Description: Delete keys and the corresponding entry from the proto table.
* Arguments:
    1. keys: the list of keys of the entries to be deleted.


```python
delete_all()
```
* Description: Delete all the contents from the proto table.

### Sharded Proto Table Storage

Sharded proto table storage will shard the data into different proto tables, denoted by `data@SHARD.pb`, where the `SHARD` is an integer that starts from `0`. In addition to these tables, there also exists a `index_map.pb` protobuf that stores the metadata information such as the mapping between each key and the shard that it belongs to, the latest shard, and the maximum size per shard.

```python
__init__(size_per_shard=None, logger=None)

```
* Description: To initialize a sharded proto table storage with `size_per_shard` for each shard.
* Arguments:
    1. size_per_shard: the size per shard. This has to be set if the sharded proto table is newly created, and can be set `None` if the table already exists.
    2. logger: please see the `storage_base` definition.


```python
read(params)
```
* Description: Read from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. proto table is a key-value storage, and therefore the params need to contain field of `key`.
* Return: the value to the key, None if key does not exist.

```python
read_multiple(params)
```
* Description: Read from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. proto table is a key-value storage, and therefore the params need to contain field of `keys`, which is a list of keys in the storage.
* Return: a dictionary of key-values, where keys might be a subset of the input keys in the params for which the key exists in the sharded proto table storage.

```python
read_all()
```
* Description: Read all the data from the storage


```python
write(data, params)
```
* Description: Write to the storage.
* Arguments:
    1. data: the write parameters.
* Explanation:
    1. The data needs to be a dictionary of `key, value` with key being a string and value being a protobuf message of any user defined
    types.
    2. The params can contain `overwrite` with its value a boolean indicating whether to overwrite the value if the key already
    exists in the proto table.

### Partitioner Storage

!!! note
    The base class implementation of partitioners is in [partitioner_base.py](https://github.com/kfrancischen/pslx/blob/master/pslx/storage/partitioner_base.py),
    it uses an underlying tree structure defined in [tree_base.py](https://github.com/kfrancischen/pslx/blob/master/pslx/core/tree_base.py).

In PSLX, five types of partitioners are supported:
1. `PartitionerStorageType.MINUTELY`: the underlying directory will be format of `2020/03/01/00/59/`.
2. `PartitionerStorageType.HOURLY`:  the underlying directory will be format of `2020/03/01/00/`.
3. `PartitionerStorageType.DAILY`:  the underlying directory will be format of `2020/03/01/`.
4. `PartitionerStorageType.MONTHLY`: the underlying directory will be format of `2020/03/`.
5. `PartitionerStorageType.YEARLY` the underlying directory will be format of `2020/`.

All the partitioners share with the following functions. The choice of partition type would depend on the data size. It is recommended
that if the data size is huge, a more fine grained storage (`PartitionerStorageType.MINUTELY`) is used, and vice versa.

```python
set_underlying_storage(storage)
```
* Description: Set underlying storage behind the partitioner.
* Arguments:
    1. storage: any storage among `StorageType.DEFAULT_STORAGE`, `StorageType.FIXED_SIZE_STORAGE`,
 and `StorageType.PROTO_TABLE_STORAGE`.

```python
set_max_capacity(max_capacity)
```
* Description: Set the maximum capacity of the partitioner.
* Arguments:
    1. max_capacity: the maximum capacity (number of file nodes) stored in the partitioner, negative meaning the partitioner
    will store all the file nodes.


```python
set_config(config)
```
* Description: Set the config for the underlying storage.
* Arguments:
    1. config: the config that is added to the existing config of the underlying storage.


```python
get_dir_name()
```
* Description: Get the directory name that the partitioner is initialized from.
* Return: the directory name.

```python
get_size()
```
* Description: Get the current size of the partitioner file tree.
* Return: the size of the partitioner


```python
is_empty()
```
* Description: Check whether the partitioner is empty (no files).
* Return: True if empty and False otherwise.


```python
get_dir_in_timestamp(dir_name)
```
* Description: Get the timestamp of the directory within the partitioner.
* Arguments:
    1. dir_name: the directory name.
* Return: The formatted datetime object.

```python
get_latest_dir()
```
* Description: Get latest directory in timestamp contained in the partitioner.
* Return: The latest directory.

```python
get_oldest_dir()
```
* Description: Get oldest directory in timestamp contained in the partitioner.
* Return: The oldest directory.

```python
get_previous_dir(cur_dir)
```
* Description: Get the previous directory with respect to the current directory.
* Arguments:
    1. cur_dir: the current directory
* Return: The previous directory if exists, otherwise None.

```python
get_next_dir(cur_dir)
```
* Description: Get the next directory with respect to the current directory.
* Arguments:
    1. cur_dir: the current directory
* Return: The next directory if exists, otherwise None.

```python
read(params)
```
* Description: Read from the latest file in the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. The underlying storage might prefer a different file name stored in each partition, hence `base_name` is an arg in
    params. The default `base_name` is `data` for `StorageType.DEFAULT_STORAGE` and `StorageType.FIXED_SIZE_STORAGE`, and `data.pb` for
    `StorageType.PROTO_TABLE_STORAGE`. One can also set `reinitialize_underlying_storage` if one wants the storage
    to be reinitialized.
    2. The read only load data from the latest directory.

```python
read_range(data, params)
```
* Description: Read a range of files from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. The params must contain `start_time` and `end_time` in order for the partitioner to retrieve files
    with partition within the given time range. The interval is a close interval.
    2. The return from this function will be a dictionary with file name as the key and file content
    as the value.
    3. If the underlying storage is a proto table, the value in the output dict will be in the format of
    `{key_1: val_1, ... ..., key_n: val_n}` with all the `val_i` being an `Any` type message.

```python
write(data, params)
```
* Description: Write to the storage.
* Arguments:
    1. data: the write parameters.
* Explanation:
    1. The underlying storage might prefer a different file name stored in each partition, hence `base_name` is an arg in
    params. The default `base_name` is `data` for `StorageType.DEFAULT_STORAGE` and `StorageType.FIXED_SIZE_STORAGE`, and `data.pb` for
    `StorageType.PROTO_TABLE_STORAGE`.
    2. If `make_partition` is in the params and it is set False, the partition will not make new partition for the new incoming data,
    otherwise (`make_partition` unset or set True), it will make partition based on the current timestamp. If an extra `timezone` field is set,
    the partitioner will make a new partition based on the timezone. Possible `timezone` could be `PST`, `EST` or `UTC`. If not set, the default
    time zone is `PST`.

!!! info
    Debug only.

```python
print_self()
```
* Description: Print the internal file tree.
