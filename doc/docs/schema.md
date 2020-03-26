PSLX uses protobuf to store data. All the schemas are in the folder of [schema](https://github.com/kfrancischen/pslx/tree/master/pslx/schema).
In the [enums.proto](https://github.com/kfrancischen/pslx/tree/master/pslx/schema/enums.proto), the following enum types are defined: 

1. `ModeType`: whether it is in test mode or production mode. The mode is controlled by the environment variable named `PSLX_TEST`,
and if in test mode, all the auto generated file will have a `TEST/` in their path, otherwise it would be `PROD/`.     
2. `SortOrder`: the order of the children nodes and parent nodes in a node.      
3. `DataModelType`: the data model type for the containers.     
4. `InstantMessagingType`: the type of instant messaging app.
5. `DiskLoggerLevel`: the level of logging, similar to the regular logging level.
6. `StorageType`: the type of the storage.
7. `ReadRuleType`: the read rule for `DefaultStorage`.
8. `WriteRuleType`: the write rule for `DefaultStorage` and `FixedSizeStorage`.
9. `PartitionerStorageType`: the type of the partitioner.
10. `Status`: status of the operator, container, and RPC response.
11. `Signal`: signal for internal usage only.

In [rpc.proto](https://github.com/kfrancischen/pslx/tree/master/pslx/schema/rpc.proto), RPC-related message types are defined:     

1. `GenericRPCService`: the generic rpc service supported in PSLX.     
2. `HealthCheckerRequest`: the health checker request.      
3. `HealthCheckerResponse`: the health checker response.       
4. `GenericRPCRequest`: generic request that encapsulates the request message in its `Any` field.        
5. `GenericRPCResponse`: generic response that encapsulates the response message in its `Any` field.        
6. `GenericRPCRequestResponsePair`: pair of generic request and response.        
7. `InstantMessagingRPCRequest`: instant messaging rpc request.         
8. `EmailPRCRequest`: email rpc request.
9. `RPCIOResponse`: RPC io rpc request.
10. `ProtoViewerRPCRequest`: request for proto viewer service.
11. `ProtoViewerRPCResponse`: response from proto viewer service.
12. `FileViewerRPCRequest`: request for file viewer service.
13. `FileViewerRPCResponse`:response from file viewer service.

In [snapshots.proto](https://github.com/kfrancischen/pslx/tree/master/pslx/schema/snapshots.proto), the snapshots for node, operator and container are defined:     

1. `NodeSnapshot`: snapshot for [node](https://github.com/kfrancischen/pslx/blob/master/pslx/core/node_base.py).
2. `OperatorSnapshot`:  snapshot for operator.
3. `ContainerSnapshot`:  snapshot for container.
4. `OperatorContentPlain`:  a plain text representation for content in operator.
5. `OperatorContentList`: a list representation for content in operator.
6: `OperatorContentDict`: a dictionary representation for content in operator

In [storage.proto](https://github.com/kfrancischen/pslx/tree/master/pslx/schema/storage.proto), the proto table storage type and the value for the container backend storage are defined:     

1. `ProtoTable`: generic storage schema for `ProtoTableStorage`.
2. `ContainerBackendValue`: the value type for container backend service.

In [common.proto](https://github.com/kfrancischen/pslx/tree/master/pslx/schema/common.proto), various credential formats are defined:     

1. `EmptyMessage`:  a dummy holder for empty message.
2. `Credentials`: a specifically designed message to storage credentials.
3. `FrontendConfig`:  the config for frontend.
