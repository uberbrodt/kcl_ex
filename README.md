# KCL

A library that implements the API for the Amazon Kinesis Client Library
Multilang Daemon. This allows you to write your kinesis message processing
logic in Elxiir while taking advantage of the Java Kinesis Client Library's 
ability to handle shards automatically.

## Install
Add this to your dependencies
```
    {:kcl, "~> 0.1.0"},
```
and run `mix deps.get`

## Usage
This is a library, so the best way to use it is with another elixir app that
is configured to run as an Escript, that can then be started by the Multilang
Daemon. Here's an example of very basic usage:

```
  defmodule MyProcessor do
    use KCL.RecordProcessor

    def process_record(data), do: "I got #{data}"
  end
```

Here, `data` is an array of arbitrary bytes. You can read more about the
Multilang API
[here](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java).


**NOTE:** This project doesn't include the multilang Java library. I suggest you
take a look at my [Kinesis
Multilang](https://github.com/uberbrodt/kinesis-multilang-app) project, as it
provides an easy way to get the java client and it's dependencies. 

## Roadmap
The list right now is:
- [ ] Replace JSX with Poison
- [ ] Test much more
- [ ] Do an example project with Kinesis Multilang
- [ ] Lock the API and move to version 1.0. 

I don't foresee the API changing much going forward, but I'm reserving that
right until I feel that it's solid enough to be 1.0.

## Contributions

Much of this work was take from the
[amazon-kinesis-client-elixir](https://github.com/colinbankier/amazon-kinesis-client-elixir).
The author was generous enough to put it under the Apache 2.0 License.
