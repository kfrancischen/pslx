### Package PSLX
**P**ython **S**tandard **L**ibrary e**X**tension.

A standard library for job scheduling, micro services including message queue, RPC, instant messaging and monitoring, tooling 
such as logging, file storage and caching. The library is written 
compatible to Python 3.7+. To use the program, please install the latest [protobuf compiler](https://github.com/protocolbuffers/protobuf)
and [gRPC](https://grpc.io/) tools. This document covers the APIs of PSLX provided, and is organized as follows:

#### Job Scheduling
* Running ad-hoc and scheduled jobs with [operators and operator containers](container.md).

#### Data Storage
* Data storage using [plain text file, in-memory fixed size file, proto table, and timestamp-based partitioner](storage.md).

#### RPC
* [General framework of RPC](rpc.md) used in PSLX.

#### Micro Services
* [Instant messaging services](micro_services/instant_messaging.md) built for Slack, Rocketchat and Microsoft Teams using the PSLX RPC framework.
* [Email service](micro_services/email.md) for sending emails through code using the PSLX RPC framework.
* [RPC storage io](micro_services/rpc_storage_io.md) for reading the data storage through remote RPC calls using the PSLX RPC framework.
* [Message Queue](micro_services/message_queue.md) for building simple message queue using proto buffers. Part of the implementation also is shared with the PSLX rpc framework.
* [PSLX Frontend](micro_services/frontend.md) Flask frontend built to monitor the health of RPC servers, container status, and browser the content of protobufs and file system for local and remote servers.

#### Tools
* [Tools](tool.md) provided by PSLX for thread-safe file io, LRC caching, ttl-ed logging, and SQL server read/write.

#### Utilities
* [Utilities](util.md) provided by PSLX for file operation, protobuf related utilities, async unittesting, timezone related utilities, 
yaml file io, text coloring, and credential composing and environment variables access.

#### TTL Cleaner
* Internally built [ttl_cleaner](ttl_cleaner.md) for temporary/ttl-ed file removing and garbage collection.

#### Proto Schema
* Internal [protobuf schemas](schema.md).

Please also take a look at the [example](https://github.com/kfrancischen/pslx/tree/master/example) folder for different example implementations.