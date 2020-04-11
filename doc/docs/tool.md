PSLX provides a set of tools to assist development, and they include

1. Logging tool for ttl-ed logging.
2. File locker to ensure the atomic io of files.
3. LRU cache for caching.
4. SQL tool for connecting to SQL database and executing queries.          
5. Timeout tool to timeout a function in the main thread.

### Documentation for Logging Tool
To create a logging tool instance, please use
```python
__init__(name, date, ttl)
```
* Arguments:
    1. name: the name of the logger instance.
    2. date: the date that this logger is created, default to the current time.
    3. ttl: the ttl policy, default to be -1.

The logger supports multiple level logging, and one can call these by using the following functions

1. `info(string)`: for level = info.

2. `warning(string)`: for level = warning.
3. `debug(string)`: for level = debug.
4. `error(string)`: for level = error.

The logger output will be the format of 

**logger name**\_**\[file name: line number, timestamp\]**\_**the logged information**

### Documentation for File Locker
FileLocker tool is used in combination within a with sentence, for instance for reading a file:
```python
with FileLocker(protected_file_path=file_name, read_mode=True):
    with open(file_name, 'r') as infile:
        ... ...
```
and for writing data to a file:
```python
with FileLocker(protected_file_path=file_name, read_mode=False):
    with open(file_name, 'w') as outfile:
        ... ...
```
FileLocker will raise error if the `protected_file_path` does not exist if `read_mode` is True.

### Documentation for Timeout Tool
The TimeoutTool can only be used in the main thread in the following way:
```python
with TimeoutTool(timeout=10):
    ... ...
```
then the enclosed program will time out after 10 seconds.

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