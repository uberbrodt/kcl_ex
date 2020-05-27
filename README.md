# KCL

Implements a native Elixir implementation of Amazon's Kinesis Client Library
(KCL). The KCL is a Java library that uses a DynamoDB table to keep track of
how far an app has processed a Kinesis stream and to correctly handle shard
splits and merges.

By using this library, you get the above functionality without the need to
deploy a the KCL Multilang Daemon.


## Install
Add this to your dependencies
```
    {:kinesis_client, "~> 0.1.0"},
```
and run `mix deps.get`

## Usage

Stream processing and acknowledgement is handled in a Broadway pipeline. Here's
a basic configuration:

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


## Development

the tests by default require
[localstack](https://github.com/localstack/localstack) to be installed and
running. How to do that is outside the scope of this readme, but here's how I'm
doing it currently:
```
SERVICES=kinesis,dynamodb localstack start --host
```

## TODO
- [ ] Test shard merges and splits more thoroughly
- [ ] Implement a work stealing algorithim to help distribute the load among
  different Elixir nodes processing the same app.

