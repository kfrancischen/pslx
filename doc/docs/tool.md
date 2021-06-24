PSLX provides a set of tools to assist development, and they include

1. LRU cache for caching.
2. SQL tool for connecting to SQL database and executing queries.
3. Mongodb tool for connection to mongodb.
4. Fetcher tool to fetch partitioned ProtoTable (whose values are of the same proto message type and keys are timestamps).
5. Watcher tool to fetch partitioned ProtoTable (whose values are of the same proto message type and keys are timestamps).
6. Registry tool to be used as decorators to register functions.

### Documentation for LRU Caching
LRU caching supports the following methods:
```python
__init__(max_capacity)
```
* Description: create a LRU cache with capacity equal max_capacity.
* Arguments:
    1. max_capacity: the maximum capacity of the LRU cache.

```python
get(key)
```
* Description: get the value stored in the key in the LRU cache.
* Arguments:
    1. key: the key of the value. If key is not found, the function will return None.
* Return: the value corresponding to the key.

```python
set(key, value)
```
* Description: set the key value pair to the cache.
* Arguments:
    1. key: the key of the pair to be inserted.
    2. value: the value of the pair to be inserted.

### Documentation for SQL Tool
SQL tool supports the following methods:
```python
__init__()
```
* Description: create an instance of the SQL tool.

```python
connect_to_database(credential, database)
```
* Description: connect to the database name with credential.
* Arguments:
    1. credential: the credential used to connect to the database.
    2. database: the name of the database that is going to be connected to.

```python
execute_query_str(query_str, modification)
```
* Description: execute the query string.
* Arguments:
    1. query_str: the string of the query to be executed.
    2. modification: boolean indicating whether the query will modify the database.

```python
execute_query_file(query_file, modification)
```
* Description: execute the query contained in a file.
* Arguments:
    1. query_file: the file that contains the query.
    2. modification: boolean indicating whether the query will modify the database.

Note that before executing any queries, please connect to a database first.

### Documentation for Mongodb Tool
Mongodb tool supports the following methods:
```python
__init__()
```
* Description: create an instance of the Mongodb tool.

```python
connect_to_database(credential)
```
* Description: connect to mongodb with credential.
* Arguments:
    1. credential: the credential used to the connect to mongodb.

```python
list_databases()
```
* Description: list all the database names.

```python
list_collections(database_name)
```
* Description: list the collection names in the database.
* Arguments:
    1. database_name: the name of the database.

```python
get_database(database_name)
```
* Decription: get the database with database name.
* Arguments:
    1. database_name: the name of the database.

```python
get_collection(database_name, collection_name)
```
* Decription: get the collection with collection name in a database.
* Arguments:
    1. database_name: the name of the database.
    2. collection_name: the name of the collection.


### Documentation for Fetcher Tool
Tool to fetch the latest, oldest data or data within a time range.

To initialize `PartitionerFetcher`
```python
__init__(partitioner, logger)
```
* Description: initialize a `PartitionerFetcher`
* Arguments:
    1. partitioner: the partitioner object to fetch. It needs to have underlying storage of a `ProtoTableStorage`.
    2. logger: the logger for the fetcher.

```python
fetch_latest()
```
* Description: fetch the latest data entry, sorted by the key.

```python
fetch_oldest()
```
* Description: fetch the oldest data entry, sorted by the key.

```python
fetch_range(start_time, end_time)
```
* Description: fetch the data whose key is within the range.


### Documentation for Watcher Tool
Tool to monitor a specific latest key.

To initialize `PartitionerWatcher`
```python
__init__(partitioner, logger, delay, timeout)
```
* Description: initialize a `PartitionerWatcher`
* Arguments:
    1. partitioner: the partitioner object to watch. It needs to have underlying storage of a `ProtoTableStorage`.
    2. logger: the logger for the watcher.
    3. delay: the seconds between each trial.
    4. timeout: the seconds of watch timeout.

```python
watch_key(key)
```
* Description: watch for the appearance of the given key in the latest partition.
* Arguments:
    1. key: the key to watch.

### Documentation for Registry Tool
The registry tool can be used as a function decorator to store functions in a dictionary. The following code shows the simple usage of this tool:

```python
from pslx.tool.registry_tool import RegistryTool

registry = RegistryTool()
@registry.register("example_func")
def example_func():
    ... ...
```
