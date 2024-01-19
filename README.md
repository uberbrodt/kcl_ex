# KCL

Implements a native Elixir implementation of Amazon's Kinesis Client Library
(KCL). The KCL is a Java library that uses a DynamoDB table to keep track of
how far an app has processed a Kinesis stream and to correctly handle shard
splits and merges.

By using this library, you get the above functionality without the need to
deploy a the KCL Multilang Daemon.

## Install

Add this to your dependencies

```elixir
    {:kinesis_client, "~> 0.1.0"},
```

and run `mix deps.get`

## Usage

Stream processing and acknowledgement is handled in a Broadway pipeline that starts
for each Kinesis shard. Here's a basic configuration:

```elixir
opts = [
  stream_name: "kcl-ex-test-stream",
  app_name: "my-test-app",
  shard_consumer: MyShardConsumer,
  processors: [
    default: [
      concurrency: 1,
      min_demand: 10,
      max_demand: 20
    ]
  ],
  batchers: [
    default: [
      concurrency: 1,
      batch_size: 40
    ]
  ]
]

KinesisClient.Stream.start_link(opts)
```

`MyShardConsumer` needs to implement the `Broadway` behaviour. You will want to
start the `KinesisClient.Stream` in your application's supervision tree.

`kcl_ex` will then work out which Kinesis shards ought to be started and then start
a broadway pipeline for each shard. Once the shard is fully consumed, the pipeline
will be stopped.

## Things to keep in mind...

If you're concerned with processing every message in your Kinesis Stream
successfully, you'll likely want to keep processor and batch concurrency set to `1`.
This is because you can only process a Kinesis stream by checkpointing where
you're at, as opposed to ack-ing individual messages like you can with SQS.
Increase the number of shards if you want to increase processing throughput.

If increasing the number of shards is not possible or desirable, I would
recommend fanning out in the `handle_batch/4` callback of your shard consumer.
Configuring dead letter queues and partitioning are dependent on your
application's requirements and the structure of your data.

## How it all works

Kinesis streams are made up of multiple shards. A shard consists of a series of
ordered records. Each record has a sequence number that is unique within the
shard. The sequence number is used to keep track of how far an app has processed
a shard. Kinesis shards can be split and merged to achieve horizontal scale. As
more writes are applied to the shards in a Kinesis stream, the shards can be
split to increase the number of shards. This allows for more read and write
throughput. If the number of shards is too high, they can be merged to reduce
the number of shards and associated costs.

To consume from a Kinesis stream, you need to keep track of how far you've
processed each shard. This is done by checkpointing the sequence number of the
last record you've processed. By default, this library uses a DynamoDB table to keep track of
this progress. This library also uses the DynamoDB table to keep track of which shards are
being processed by which nodes when running multiple copies of the same consumer
in a horizontally scaled system.

### On startup

When the `KinesisClient.Stream` module starts, it boots up two processes:

1. A Dynamic Supervisor for handling each shard that comes and goes
2. A [KinesisClient.Stream.Coordinator](lib/kinesis_client/stream/coordinator.ex) process that
   handles starting shards that are ready to be processed. As shards split and merge, they notify the coordinator process and then shut themselves down. The coordinator then handles adding those new shards to the pipeline.
3. Each [KinesisClient.Stream.Shard](lib/kinesis_client/stream/shard.ex) supervisor starts a [KinesisClient.Stream.Lease](lib/kinesis_client/stream/shard/lease.ex) to take and maintain exclusive access to the Kinesis shard. By default, it uses the DynamoDB table to record which shard is being processed by which node, and the sequence number of the most recently processed record.
4. Each `Shard` also starts a [`Pipeline`](lib/kinesis_client/stream/shard/pipeline.ex) process. This sets up the Broadway pipeline that will process the given Shard (although it starts in a `stopped` state, and waits for the `Lease` to send a message indicating it is ready to start processing). The Broadway pipeline producer module is the `KinesisClient.Stream.Shard.Producer` module, which is responsible for fetching records from the Kinesis shard and passing them to the Broadway pipeline. The `Pipeline` delegates the broadway callbacks to the `shard_consumer` module passed in as an option to the `KinesisClient.Stream` module.

## Development

The tests by default require [localstack](https://github.com/localstack/localstack) to be installed and
running. How to do that is outside the scope of this readme, but here's how I'm
doing it currently:

```shell
SERVICES=kinesis,dynamodb localstack start --host
```

## TODO

- [x] Test shard merges and splits more thoroughly
- [ ] Adaptively respond to rate limits from Kinesis and GenStage demand so that we don't get throttled
- [ ] Support [enhanced fanout](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html) for higher throughput
- [ ] Support different shard iterator types (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax) and configuring them in the `KinesisClient.Stream` module
- [ ] Support more than just Dynamo for tracking state (e.g. Redis or Postgres)
- [ ] Consider integrating with Phoenix PubSub so that shard splits and merges can be broadcast to other nodes in the cluster, which can then start/stop the relevant Broadway pipelines.
- [ ] Consider starting just one Broadway pipeline, and having shards feed into the same pipeline. This would make it operate more like other broadway adapters, since kcl_ex wraps broadway with its own supervision tree.
- [ ] Implement a work stealing algorithm to help distribute the load among different Elixir nodes processing the same app.
  - One option here would be to include a function that decides if a node ought to process a shard or not. This function could be passed in as an option to the `KinesisClient.Stream` module. Such a function could then assign shards to nodes based on a hash of the shard id, for example. Nodes
    would only start the shard if the function returns true. Multiple nodes could return true, and the
    leasing mechanism would ensure the shards are only processed by one node at a time.
