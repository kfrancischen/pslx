PSLX provides a comprehensive set of utility functions including

1. [file_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/file_util.py): utilities related to files and directories.
2. [proto_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/proto_util.py): utilities for protobuf.
3. [timezone_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/timezone_util.py): utilities for timestamp related.
4. [dummy_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/dummy_util.py): a wrapper of dummy tools.
5. [yaml.util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/yaml_util.py): utility for yaml files.
6. [common_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/common_util.py): utility for creating credentials.
7. [env_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/env_util.py): utility for PSLX built-in environment variables.
8. [decorator_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/decorator_util.py): utility for commonly used
decorators.


!!! info
    All the below functions are labeled as `classmethods`, and can be called without initiating any instance of the class.

### Documentation of File Utilities
The following are functions related to files and directories.

```python
base_name(file_name)
```
* Description: get the base name of a file.
* Arguments:
    1. file_name: the name of the file.

```python
dir_name(file_name)
```
* Description: get the directory of a file.
* Arguments:
    1. file_name: the name of the file.

```python
does_file_exist(file_name)
```
* Description: check whether a file exist or not.
* Arguments:
    1. file_name: the name of the file.

```python
does_dir_exist(dir_name)
```
* Description: check whether a directory exist or not.
* Arguments:
    1. dir_name: the name of the directory.

```python
is_file_empty(file_name)
```
* Description: check whether a file is empty.
* Arguments:
    1. file_name: the name of the file.

```python
is_dir_empty(dir_name)
```
* Description: check whether a file is empty.
* Arguments:
    1. dir_name: the name of the directory.

```python
create_file_if_not_exist(file_name)
```
* Description: create a file if the file does not exist
* Arguments:
    1. file_name: the name of the file.

```python
create_dir_if_not_exist(dir_name)
```
* Description: create a directory if the file does not exist
* Arguments:
    1. dir_name: the name of the directory.

```python
die_if_file_not_exist(file_name)
```
* Description: raise error if a file does not exist.
* Arguments:
    1. file_name: the name of the file.

```python
die_if_dir_not_exist(dir_name)
```
* Description: raise error if a directory does not exist.
* Arguments:
    1. dir_name: the name of the directory.

```python
is_file(path_name)
```
* Description: check whether the given path is a file.
* Arguments:
    1. path_name: the name of the path.

```python
is_dir(path_name)
```
* Description: check whether the given path is a directory.
* Arguments:
    1. path_name: the name of the path.

```python
normalize_file_name(file_name)
```
* Description: normalize a file name.
* Arguments:
    1. file_name: the name of the file.

```python
normalize_dir_name(dir_name)
```
* Description: normalize a directory name.
* Arguments:
    1. dir_name: the name of the directory.

```python
list_files_in_dir(dir_name)
```
* Description: list all files in a directory.
* Arguments:
    1. dir_name: the name of the directory.

```python
list_dirs_in_dir(dir_name)
```
* Description: list all sub directories in a directory.
* Arguments:
    1. dir_name: the name of the directory.

```python
list_files_in_dir_recursively(proto_message)
```
* Description: list all files in a directory recursively.
* Arguments:
    1. dir_name: the name of the directory.

```python
list_dirs_in_dir_recursively(proto_message)
```
* Description: list all sub directories in a directory recursively.
* Arguments:
    1. dir_name: the name of the directory.

```python
remove_file(file_name)
```
* Description: remove a file.
* Arguments:
    1. file_name: the name of the file to be removed.

```python
remove_dir_recursively(dir_name)
```
* Description: remove sub directories recursively from a directory.
* Arguments:
    1. dir_name: the name of the directory.

```python
get_file_modified_time(file_name)
```
* Description: get the last modified time of a file.
* Arguments:
    1. file_name: the name of the file.

```python
get_ttl_from_path(path_name)
```
* Description: get the ttl from a path.
* Arguments:
    1. path_name: the path, a file or directory.

```python
join_paths_to_file_with_mode(root_dir, base_name, ttl)
```
* Description: join root directory, base name, ttl, and the mode, to a file name.
* Arguments:
    1. root_dir: the root directory of the file.
    2. base_name: the base name of the file.
    3. ttl: the ttl of the file.

```python
join_paths_to_dir_with_mode(root_dir, base_name, ttl)
```
* Description: join root directory, base name, ttl, and the mode, to a directory name.
* Arguments:
    1. root_dir: the root directory of the directory.
    2. base_name: the base name of the directory.
    3. ttl: the ttl of the file.

```python
join_paths_to_file(root_dir, base_name, ttl)
```
* Description: join root directory, base name, and ttl, to a file name.
* Arguments:
    1. root_dir: the root directory of the file.
    2. base_name: the base name of the file.
    3. ttl: the ttl of the file.

```python
join_paths_to_dir(root_dir, base_name, ttl)
```
* Description: join root directory, base name, and ttl, to a directory name.
* Arguments:
    1. root_dir: the root directory of the directory.
    2. base_name: the base name of the directory.
    3. ttl: the ttl of the file.

```python
get_file_names_from_pattern(pattern)
```
* Description: get all the files with the pattern.
* Arguments:
    1. pattern: the pattern of the files.

```python
get_mode(path_name)
```
* Description: get the mode from a path.
* Arguments:
    1. path_name: the path to a file or directory.

```python
write_proto_to_file(proto, file_name)
```
* Description: write a proto message to a file.
* Arguments:
    1. proto: the proto message to be written.
    2. file_name: the output file name.

```python
read_proto_from_file(proto_type, file_name)
```
* Description: read a proto message with given type from a file.
* Arguments:
    1. proto_type: the type of the proto message.
    2. file_name: the name of the file containing the message.

```python
create_container_snapshot_pattern(container_name, container_class, container_ttl)
```
* Description: get the container snapshot pattern.
* Arguments:
    1. container_name: the name of the container.
    2. container_class: the class of the container.
    3. container_ttl: the ttl of the container.

```python
create_operator_snapshot_pattern(container_name, operator_name, container_class, container_ttl)
```
* Description: get the operator snapshot pattern.
* Arguments:
    1. container_name: the name of the container.
    2. operator_name: the name of the operator.
    3. container_class: the class of the container.
    4. container_ttl: the ttl of the container.

```python
get_file_size(file_name)
```
* Description: get a string representation of the size of the file.
* Arguments:
    1. file_name: the name of the file.

### Documentation of Protobuf Utilities
THe following functions are related to protobuf.

```python
check_valid_enum(enum_type, value)
```
* Description: check whether a enum type value is valid.
* Arguments:
    1. enum_type: the enum type of the value.
    2. value: the value to be checked.

```python
get_name_by_value(enum_type, value)
```
* Description: get the name of the enum type value.
* Arguments:
    1. enum_type: the enum type of the value.
    2. value: the value to be examined.

```python
get_value_by_name(enum_type, name)
```
* Description: get the value of the enum type name.
* Arguments:
    1. enum_type: the enum type of the name.
    2. name: the name to be examined.

```python
get_name_by_value_and_enum_name(enum_type_str, value)
```
* Description: get the name of the enum type value when the enum type is represented by a string.
* Arguments:
    1. enum_type_str: the string representation of the enum type.
    2. value: the value to be examined.

```python
get_value_by_name_and_enum_name(enum_type_str, name)
```
* Description: get the value of the enum type value when the enum type is represented by a string.
* Arguments:
    1. enum_type_str: the string representation of the enum type.
    2. name: the name to be examined.

```python
message_to_json(proto_message)
```
* Description: convert a proto message to json.
* Arguments:
    1. proto_message: the proto message to be converted.

```python
message_to_string(proto_message)
```
* Description: convert a proto message to plain string.
* Arguments:
    1. proto_message: the proto message to be converted.

```python
message_to_text(proto_message)
```
* Description: convert a proto message to text.
* Arguments:
    1. proto_message: the proto message to be converted.

```python
json_to_message(message_type, json_str)
```
* Description: convert a json to a proto message in a given type.
* Arguments:
    1. message_type: the type of the output proto message.
    2. json_str: the json to be converted.

```python
string_to_message(message_type, string)
```
* Description: convert a plain string to a proto message in a given type.
* Arguments:
    1. message_type: the type of the output proto message.
    2. string: the plain string to be converted.

```python
text_to_message(message_type, text_str)
```
* Description: convert a text to a proto message in a given type.
* Arguments:
    1. message_type: the type of the output proto message.
    2. text_str: the text to be converted.

```python
message_to_any(message)
```
* Description: convert a proto message to `Any` type.
* Arguments:
    1. message: the message to be converted.

```python
any_to_message(message_type, any_message)
```
* Description: convert an `Any` type message to a given type message.
* Arguments:
    1. message_type: the type of the output proto message.
    2. any_message: the `Any` message to be converted.

```python
infer_message_type_from_str(message_type_str, modules)
```
* Description: infer the message type represented by a string.
* Arguments:
    1. message_type_str: the string representation of the message type.
    2. modules: the possible proto modules to search.

```python
infer_str_from_message_type(message_type)
```
* Description: infer the string representation of a given message type.
* Arguments:
    1. message_type: the proto message type.

### Documentation of Timezone Utilities
The timestamp related utility class includes the following methods:

```python
utc_to_pst(utc_time)
```
* Description: convert a utc datetime object to a pst datetime object.
* Arguments:
    1. utc_time: the utc datetime object.

```python
utc_to_est(utc_time)
```
* Description: convert a utc datetime object to an est datetime object.
* Arguments:
    1. utc_time: the utc datetime object.

```python
pst_to_est(western_time)
```
* Description: convert a pst datetime object to an est datetime object.
* Arguments:
    1. western_time: the pst datetime object.

```python
est_to_pst(eastern_time)
```
* Description: convert an est datetime object to a pst datetime object.
* Arguments:
    1. eastern_time: the est datetime object.

```python
cur_time_in_local()
```
* Description: the current time in local timezone.

```python
cur_time_in_utc()
```
* Description: the current time in utc timezone.

```python
cur_time_in_pst()
```
* Description: the current time in pst timezone.

```python
cur_time_in_est()
```
* Description: the current time in est timezone.

```python
cur_time_in_est(time_str)
```
* Description: convert a string representation of datetime object to datetime object.
* Arguments:
    1. time_str: the string representation of datetime object.

### Documentation of Dummy Utilities
The `DummyUtil` class is a wrapper of a few dummy entities that can be used. Here dummy means that any method inside them
are dummy function calls.
```python
dummy_logging()
```
* Description: return a dummy logger that acts as a placeholder but does not log.

!!! note
    The above operators are very helpful for containers that have only one meaningful operator. Please check
    [batch_container_example](https://github.com/kfrancischen/pslx/tree/master/example/batch_example) and
    [streaming_container_example](https://github.com/kfrancischen/pslx/tree/master/example/streaming_example).

```python
dummy_streaming_operator(operator_name)
```
* Description: return a dummy streaming operator tha does not execute anything.
* Arguments:
    1. operator_name: the name of the operator.

```python
dummy_batch_operator(operator_name)
```
* Description: return a dummy batch operator tha does not execute anything.
* Arguments:
    1. operator_name: the name of the operator.

### Documentation of Yaml Utilities
The following method is provided
```python
yaml_to_dict(file_name)
```
* Description: read a yaml file to dictionary.
* Arguments:
    1. file_name: the yaml file name.
* Return: the dictionary representation of the content in the yaml file.

### Documentation of Common Utilities
The following methods are provided
```python
make_email_credentials(email_addr, password, email_server, email_server_port)
```
* Description: make a credential for email RPC server.
* Arguments:
    1. email_addr: the email address.
    2. password: the password to the email.
    3. email_server: the smtp server of the email, default to be "smtp.gmail.com".
    4. email_server_port: the port for the email server, default to be 25.

```python
make_sql_server_credentials(sql_host_ip, sql_port, user_name, password)
```
* Description: make a credential for SQL server.
* Arguments:
    1. sql_host_ip: the host server ip for the SQL database.
    2. sql_port: the port on the server ip for the SQL database.
    3. user_name: the user name to login the SQL database.
    4. password: the password to login the SQL database.

```python
make_frontend_config(yaml_path)
```
* Description: make a credential for PSLX frontend.
* Arguments:
    1. yaml_path: the path to the config yaml file. Please check section [frontend](micro_services/frontend.md).

### Documentation of Environment Utilities
The environment variables inside PSLX are defined in a dictionary:
```python
PSLX_ENV_TO_DEFAULT_MAP = {
    'PSLX_INTERNAL_TTL': 7,
    'PSLX_INTERNAL_CACHE': 100,
    'PSLX_TEST': False,
    'PSLX_LOG': False,
    'PSLX_DATABASE': 'database/',
    'PSLX_GRPC_MAX_MESSAGE_LENGTH': 512 * 1024 * 1024,  # 512MB,
    'PSLX_GRPC_TIMEOUT': 1,  # 1 second
    'PSLX_FRONTEND_CONFIG_PROTO_PATH': '',
    "PSLX_RPC_FLUSH_RATE": 1,
    'PSLX_RPC_PASSWORD': 'admin',
}
```
, and this utility allows user to access the above variables by
```python
get_pslx_env_variable(var, fallback_value)
```
* Arguments:
    1. var: the environment variable name.
    2. fallback_value: the fallback value if the var does not have a default value defined by PSLX.


### Documentation of Decorator Utilities

The decorator utility currently supports adding a runtime range in PST timezone to a function. For example:
```python

@DecoratorUtil.pst_runtime(weekdays=[5])
def test_func(test_string):
    return test_string


if __name__ == "__main__":
    print(test_func("test here"))
```
Then this function will only print out the string on Saturday. The decorator also supports adding ranges to hours and minutes.
