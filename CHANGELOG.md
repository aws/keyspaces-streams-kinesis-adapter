### Latest Release (v1.1.0)
* Discovers child shards at shard-end via GetStream with a ShardFilter instead of waiting for the next periodic shard sync, reducing processing latency after a shard split.
* Makes lease cleanup faster by removing the six-hour retention check. Parent leases are deleted once all child shards have begun processing.
* Adds support for the CQL duration data type.
* Upgrades Amazon Kinesis Client Library (KCL) to version 3.5.1.
* Upgrades AWS Java SDK to version 2.42.4.
* Upgrades jackson-datatype-jsr310 to version 2.21.5 to match the jackson-databind version required by KCL 3.5.1.

### Release (v1.0.0)
* Initial release of the Keyspaces Streams Kinesis Adapter.
* Implements the AWS SDK v2 `KinesisAsyncClient` interface to enable Amazon Kinesis Client Library (KCL) 3.x to consume Change Data Capture (CDC) events from Amazon Keyspaces table streams.
* Provides `StreamsSchedulerFactory` for creating a KCL Scheduler optimized for Keyspaces Streams.
