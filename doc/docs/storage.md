PSLX supports in total four different types of storage: `StorageType.DEFAULT_STORAGE`, `StorageType.FIXED_SIZE_STORAGE`,
`StorageType.PROTO_TABLE_STORAGE` and `StorageType.PARTITIONER_STORAGE`, and the `StorageType.PARTITIONER_STORAGE` also support
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
    1. logger: the logging tool (see [tool](tool.md)) for this storage. Default value is None.
    
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
    
### Fixed Size Storage Documentation

```python       
__init__(logger=None, fixed_size=-1)
```
* Description: Overrides the default constructor.
* Arguments:
    1. logger: the logging tool (see [tool](tool.md)) for this storage. Default value is None.
    2. fixed_size: the maximum data size that this storage will hold in memory, negative meaning the maximum size is infinity. 

```python       
read(params)
```
* Description: Read from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. Like the default storage, the underlying file needs to be a text file. The difference between fixed size storage and default storage
    is that fixed size storage will hold data in memory (while default storage will by default read the underlying file). The params supported here are `num_line`
    and `force_load`. `num_line` indicates the number of lines to read. If `force_load` if False and the `num_lines` exceeds the internal maximum
    size, error will be raised. If `force_load` is true, the storage will search for the file.
    2. Due to the nature of the fixed size storage, it could be used a buffer storage between disk and application.

```python       
write(data, params)
```
* Description: Write to the storage.
* Arguments:
    1. data: the write parameters.
* Explanation:
    1. Same as the writer implementation of default storage.
    
### Proto Table Storage

```python       
read(params)
```
* Description: Read from the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. proto table is a key-value storage, and therefore the params need to contain field of `key`. 
    2. The value of the proto table is a proto message, and therefore the params need to contain the field of `message_type`
    so that the reader could correctly deserialize to the desired protobuf.
    3. Under PSLX convention, the underlying file name needs to end with `.pb`.

```python
read_all()
```
* Description: Read all the data.
* Return: the key, value pairs of the table, with value being the `Any` proto format.

```python       
write(data, params)
```
* Description: Write to the storage.
* Arguments:
    1. data: the write parameters.
* Explanation:
    1. The data needs to be a list of `[key, value]` with key being a string and value being a protobuf message of any user defined
    types.
    2. The params can contain `overwrite` with its value a boolean indicating whether to overwrite the value if the key already 
    exists in the proto table.
    
```python       
delete(key)
```
* Description: Delete key and the corresponding entry from the proto table.
* Arguments:
    1. key: the key of the entry to be deleted.
 
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
read(params)
```
* Description: Read from the latest file in the storage.
* Arguments:
    1. params: the read parameters.
* Explanation:
    1. The underlying storage might prefer a different file name stored in each partition, hence `base_name` is an arg in
    params. The default `base_name` is `data` for `StorageType.DEFAULT_STORAGE` and `StorageType.FIXED_SIZE_STORAGE`, and `data.pb` for
    `StorageType.PROTO_TABLE_STORAGE`.
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
    `[key_1, val_1, ... ..., key_n, val_n]`.
    
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
    2. If `make_partition` is in the params and it is set False, the partition will not make new parittion for the new incoming data,
    otherwise (`make_partition`unset or set True), it will make partition based on the current PST timestamp.
    
!!! info
    Debug only.
    
```python       
print_self()
```
* Description: Print the internal file tree.
