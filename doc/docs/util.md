PSLX provides a comprehensive set of utility functions including

1. [file_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/file_util.py): utilities related to files and directories.
2. [proto_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/proto_util.py): utilities for protobuf.
3. [timezone_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/timezone_util.py): utilities for timestamp related.
4. [dummy_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/dummy_util.py): a wrapper of dummy tools.
5. [yaml.util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/yaml_util.py): utility for yaml files.
6. [common_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/common_util.py): utility for creating credentials.
7. [env_util.py](https://github.com/kfrancischen/pslx/blob/master/pslx/util/env_util.py): utility for PSLX built-in environment variables.

!!! info
    All the below functions are labeled as `classmethods`, and can be called without initiating any instance of the class.

### Documentation of File Utilities
### Documentation of Protobuf Utilities
### Documentation of Timezone Utilities
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
    'PSLX_BACKEND_STORAGE': 'backend/',
    'PSLX_FRONTEND_CONFIG_PROTO_PATH': '',
}
```
, and this utility allows user to access the above variables by
```python
get_pslx_env_variable(var, fallback_value)
```
* Arguments:
    1. var: the environment variable name.
    2. fallback_value: the fallback value if the var does not have a default value defined by PSLX.
