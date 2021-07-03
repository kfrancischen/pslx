### Package PSLX
**P**ython **S**tandard **L**ibrary e**X**tension.

[![996.icu](https://img.shields.io/badge/link-996.icu-red.svg)](https://996.icu)

The site for detailed documentation: https://kfrancischen.github.io/pslx/.

A standard library for job scheduling, micro services including message queue, RPC, instant messaging and monitoring, and tooling, developed by [Kaifeng Chen](https://www.linkedin.com/in/kaifeng-chen-b37a2b69/). The library is written
compatible to Python 3.7+, and is integrated upon the distributed file system [Galaxy](https://github.com/kfrancischen/galaxy). To use the program, please install the latest [protobuf compiler](https://github.com/protocolbuffers/protobuf)
and [gRPC](https://grpc.io/) tools. This document covers the APIs of PSLX provided, and is organized as follows:

#### Job Scheduling
* Running ad-hoc and scheduled jobs with [operators and operator containers](doc/docs/container.md).

#### Data Storage
* Data storage using [plain text file, proto table, sharded proto table and timestamp-based partitioner](doc/docs/storage.md).

#### RPC
* [General framework of RPC](doc/docs/rpc.md) used in PSLX.

#### Micro Services
* [Instant messaging services](doc/docs/micro_services/instant_messaging.md) built for Slack, Rocketchat and Microsoft Teams using the PSLX RPC framework.
* [Email service](doc/docs/micro_services/email.md) for sending emails through code using the PSLX RPC framework.
* [Message Queue](doc/docs/micro_services/message_queue.md) for building simple message queue using proto buffers. Part of the implementation also is shared with the PSLX rpc framework.
* [Publisher/Subscriber](doc/docs/micro_services/pubsub.md) for building simple publisher/subscriber applications.
* [PSLX Frontend](doc/docs/micro_services/frontend.md) Flask frontend built to monitor the health of RPC servers, container status, and browser the content of protobufs.

#### Tools
* [Tools](doc/docs/tool.md) provided by PSLX for galaxy-based file io, LRC caching, SQL server read/write, Mongodb tool, timestamp key-ed partitioner fetcher and watcher, function registration.

#### Utilities
* [Utilities](doc/docs/util.md) provided by PSLX for file operation, protobuf related utilities, async unittesting, timezone related utilities,
yaml file io, text coloring, and credential composing, environment variables access and decorators.

#### Proto Schema
* Internal [protobuf schemas](doc/docs/schema.md).

Please also take a look at the [example](https://github.com/kfrancischen/pslx/tree/master/example) folder for different example implementations.

I do accept BTC/ETH donation. My BTC address is 3LH5VQ51fToFTpb46N4hYn225e6iMDPU7f, and ETH address is 0xd16B5edEc78F9C798d3aD4D94b008ED091722bEC.