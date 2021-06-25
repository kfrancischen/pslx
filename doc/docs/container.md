PSLX built-in container implements a DAG connected [graph structure](https://github.com/kfrancischen/pslx/blob/master/pslx/core/graph_base.py).
For each container (graph), the node is defined as an operator, and therefore, each operator contains a dictionary of
its children operators and parent operators. The execute flow of the container will then start from the root and finish
after all the leaf operators are done.

Each operator defined in a container will have status of `Status.IDLE`, `Status.WAITING`, `Status.RUNNING`, `Status.SUCCEEDED` and `Status.FAILED`. The operator
will be in `Status.IDLE` if its parents are not waiting for being executed, and will be in `Status.WAITING` if its parents in the process
of being executed now. Then it will turn to the status of `Status.RUNNING` when all its parents are done executing, and its final
status could be one of `Status.SUCCEEDED` and `Status.FAILED` depending on the execution. One failure in the container graph will result in
failure in all the subsequent operators.

PSLX has two kinds of data models: `DataModelType.BATCH` and `DataModelType.STREAMING`, and the enums are defined as protobuf enums (Please check
the section of [schema](schema.md)). `DataModelType.BATCH` mode supports running operators in multi-thread fashion, and allows each operator to send
its status to the backend, while `DataModelType.STREAMING` only supports sending status to the backend when the container starts and ends. For the execution,
`DataModelType.STREAMING` allows operators to fail in the middle, while for `DataModelType.BATCH`, one failure in an operator will result in the failure of
 the overall container. The two types of operations all have the following functions:


### Operator Documentation

!!! note
    The base implementation of operator is in [operator_base.py](https://github.com/kfrancischen/pslx/blob/master/pslx/core/operator_base.py),
    and the ones for the `DataModelType.BATCH` and `DataModelType.STREAMING` are in [batch/operator.py](https://github.com/kfrancischen/pslx/blob/master/pslx/batch/operator.py) and
    [streaming/operator.py](https://github.com/kfrancischen/pslx/blob/master/pslx/streaming/operator.py), respectively.

```python
__init__(operator_name, logger)
```
* Description: Construct an operator.
* Arguments:
    1. operator_name: the name of the operator.
    2. logger: the logger for the operator.

```python
set_data_model(model)
```
* Description: change the data model to a different model.
* Arguments:
    1. model: the new data model, one of `DataModelType.BATCH` and `DataModelType.STREAMING`.

```python
unset_data_model()
```
* Description: Unset the preset data model to `DataModelType.DEFAULT`.

```python
get_data_model()
```
* Description: Get the model of the operator.
* Return: data model of the operator, one of `DataModelType.BATCH`, `DataModelType.STREAMING` and `DataModelType.DEFAULT`.

```python
set_status(status)
```
* Description: Set the status of the operator.
* Arguments:
    1. status: the new status, one of `Status.IDLE`, `Status.WAITING`, `Status.RUNNING`, `Status.SUCCEEDED` and `Status.FAILED`.

```python
unset_status()
```
* Description: Unset the status to `Status.IDLE`.

```python
unset_dependency()
```
* Description: Remove all the dependencies related to this operator, including removing children and parents, and the parents
and children also will remove the operator.

```python
get_status()
```
* Description: Get the status of the operator.
* Return: the status of the operator, one of `Status.IDLE`, `Status.WAITING`, `Status.RUNNING`, `Status.SUCCEEDED` and `Status.FAILED`.


```python
counter_increment(counter_name)
```
* Description: increment the counter name by 1.
* Arguments:
    1. counter_name: the name of the counter. The final counter name would be in the format of `operator_name:counter_name`.

```python
counter_increment_by_n(counter_name, n)
```
* Description: increment the counter name by n (n > 0).
* Arguments:
    1. counter_name: the name of the counter. The final counter name would be in the format of `operator_name:counter_name`.
    2. n: the increment amount.

```python
mark_as_done()
```
* Description: Mark the status of the operator as `Status.SUCCEEDED`.

```python
mark_as_persistent()
```
* Description: Mark the content of the operator to be persistent, and if true, the content will be stored in the snapshot.

```python
get_content()
```
* Description: Get the content contained in the operator.
* Return: the content of the operator if the operator is persistent, otherwise None.

```python
set_content(content)
```
* Description: Set the content of the operator.
* Arguments:
    1. content: the new content, needed to be a protobuf message whose type can be any user defined one.

```python
is_done()
```
* Description: Check whether the operator status is `Status.SUCCEEDED`.
* Return: True if the operator status is `Status.SUCCEEDED`, otherwise False.

```python
get_content_from_dependency(dependency_name)
```
* Description: Get the content from the dependency with name equal to the dependency name.
* Arguments:
    1. dependency_name: the name of the dependency.
* Return: Proto message contained in the operator with dependency name.

```python
get_content_from_snapshot(snapshot_file, message_type):
```
* Description: Get the content from a snapshot file.
* Arguments:
    1. snapshot_file: the file name of the operator snapshot file.
    2. message_type: the type of the content message.
* Return: Proto message contained in the operator snapshot file in the format of the given message type.

```python
get_status_from_snapshot(operator_name)
```
* Description: Get the status from a snapshot file.
* Arguments:
    1. snapshot_file: the file name of the operator snapshot file.
* Return: Status contained in the operator snapshot file.

```python
wait_for_upstream_status()
```
* Description: Get the upstream unfinished operator names.
* Return: A list of unfinished operator names from upstream.

```python
is_data_model_consistent()
```
* Description: Check whether the models of the children and parents and the model of self are consistent.
* Return: True if consistent, otherwise False.

```python
is_status_consistent(operator_name, order=SortOrder.ORDER)
```
* Description: Check whether the status of the children and parents and the status of self are consistent.
* Return: True if consistent, otherwise False.

```python
execute()
```
* Description: Execute the operation implemented in the operator.

```python
set_config(config)
```
* Description: Set the configuration of the operator. The built-in config will then add the config, and this function
can be used as an entry point to control parameters in the operator.
* Arguments:
    1. config: a dictionary containing key-value configurations. One common argument is `save_snapshot`, and it is true,
     the snapshot of the operator will be saved, and vice versa. The default value for this argument is False.

!!! info
    Function that needs to be implemented by any client.

```python
execute_impl()
```
* Description: Implement the operators conducted by the operator.


### Container Documentation

!!! note
    The base implementation of container is in [contain_base.py](https://github.com/kfrancischen/pslx/blob/master/pslx/core/container_base.py),
    and the ones for the `DataModelType.BATCH` and `DataModelType.STREAMING` are in [batch/container.py](https://github.com/kfrancischen/pslx/blob/master/pslx/batch/container.py) and
    [streaming/container.py](https://github.com/kfrancischen/pslx/blob/master/pslx/streaming/container.py), respectively.

```python
__init__(container_name, logger)
```
* Description: Construct an container with given name. Note that please make sure all the container names are unique across your application.
* Arguments:
    1. container_name: name of the container.
    2. logger: the logger that record events.

```python
bind_backend(server_url)
```
* Description: Binds to the container backend, described in the [frontend](micro_services/frontend.md) section.
* Arguments:
    1. server_url: the url to the rpc server.

```python
initialize(force=False)
```
* Description: Initialize the container. Once a container is initialized, it is then ready for execution. An uninitialized
container cannot be executed.
* Arguments:
    1. force: whether to force the status and data model of its contained operators to be consistent with self.

```python
set_status(status)
```
* Description: Set the status of the container.
* Arguments:
    1. status: the new status, one of `Status.IDLE`, `Status.WAITING`, `Status.RUNNING`, `Status.SUCCEEDED` and `Status.FAILED`.

```python
unset_status()
```
* Description: Unset the status to `Status.IDLE`.

```python
uninitialize()
```
* Description: Uninitialize the container.

```python
add_operator_edge(from_operator, to_operator)
```
* Description: Add a dependency edge from a from_operator to a to_operator in the container.
* Arguments:
    1. from_operator: the upstream operator.
    2. to_operator: the downstream operator.

```python
add_upstream_op(op_snapshot_file_pattern)
```
* Description: Add an upstream operator outside the container. The container then will monitor the change of the snapshot of
the operator until the status becomes `Status.SUCCEEDED`.
* Arguments:
    1. op_snapshot_file_pattern: the snapshot file pattern of the upstream operator.


```python
execute(is_backfill, num_threads)
```
* Description: Execute the container graph.
* Arguments:
    1. is_backfill: whether the execution runs in backfill mode. If so, the successful operators will be executed again.
    2. num_threads: number of threads used for the execution. If the container is of type `DataModelType.STREAMING`, this
    input will be ignored and only one thread is allowed.

!!! note
    The following function is only for `CronBatchContainer` and `IntervalBatchContainer`.

```python
execute_now(is_backfill, num_threads)
```
* Description: Execute the container graph now for once. Then there is not wait for the schedule.
* Arguments:
    1. is_backfill: whether the execution runs in backfill mode. If so, the successful operators will be executed again.
    2. num_threads: number of threads used for the execution.


 Also for `CronBatchContainer` and `CronStreamingContainer`, we do have

```python
add_schedule(day_of_week, hour, minute=None, second=None, misfire_grace_time=None)
```
* Description: Add cron like schedule to the container. There could be multiple if schedules.
* Arguments: please check the related arguments in [apscheduler](https://apscheduler.readthedocs.io/en/stable/modules/triggers/cron.html).

For `IntervalBatchContainer` and `IntervalStreamingContainer`, we do have

```python
add_schedule(days, hours=0, minutes=0, seconds=0, misfire_grace_time=None)
```
* Description: Add interval schedule to the container. There could be multiple if schedules.
* Arguments: please check the related arguments in [apscheduler](https://apscheduler.readthedocs.io/en/stable/modules/triggers/interval.html#module-apscheduler.triggers.interval).

In addition, there exist two other containers `NonStoppingBatchContainer` and `NonStoppingStreamingContainer` that will continously
execute again immediately after one successful execution.
