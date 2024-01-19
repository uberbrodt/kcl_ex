defmodule KinesisClient.KinesisResponses do
  @moduledoc false
  def shard_object do
    %{
      "HashKeyRange" => %{
        "EndingHashKey" => "56713727820156410577229101238628035241",
        "StartingHashKey" => "0"
      },
      "SequenceNumberRange" => %{
        "StartingSequenceNumber" => "49606474897903670380967043048643675070254557697658585090"
      },
      "ShardId" => "shardId-000000000000"
    }
  end

  def describe_stream(opts \\ []) do
    %{
      "StreamDescription" => %{
        "HasMoreShards" => Keyword.get(opts, :has_more_shards, false),
        "Shards" => shard_listing(),
        "StreamStatus" => Keyword.get(opts, :stream_status, "ACTIVE")
      }
    }
  end

  def describe_stream_split(opts \\ []) do
    %{
      "StreamDescription" => %{
        "HasMoreShards" => Keyword.get(opts, :has_more_shards, false),
        "Shards" => [
          %{
            "HashKeyRange" => %{
              "EndingHashKey" => "170141183460469231731687303715884105727",
              "StartingHashKey" => "0"
            },
            "SequenceNumberRange" => %{
              "EndingSequenceNumber" => "49606519059099304615023968125168557725133605139493421058",
              "StartingSequenceNumber" => "49606519059088154242424702813598998791816825219170959362"
            },
            "ShardId" => "shardId-000000000000"
          },
          %{
            "HashKeyRange" => %{
              "EndingHashKey" => "340282366920938463463374607431768211455",
              "StartingHashKey" => "170141183460469231731687303715884105728"
            },
            "SequenceNumberRange" => %{
              "StartingSequenceNumber" => "49606519059110454987623233436740534510089473580676939794"
            },
            "ShardId" => "shardId-000000000001"
          },
          %{
            "HashKeyRange" => %{
              "EndingHashKey" => "85070591730234615865843651857942052862",
              "StartingHashKey" => "0"
            },
            "ParentShardId" => "shardId-000000000000",
            "SequenceNumberRange" => %{
              "StartingSequenceNumber" => "49606519424864976988723983581067849899795321077725397026"
            },
            "ShardId" => "shardId-000000000002"
          },
          %{
            "HashKeyRange" => %{
              "EndingHashKey" => "170141183460469231731687303715884105727",
              "StartingHashKey" => "85070591730234615865843651857942052863"
            },
            "ParentShardId" => "shardId-000000000000",
            "SequenceNumberRange" => %{
              "StartingSequenceNumber" => "49606519424887277733922514204209385618067969439231377458"
            },
            "ShardId" => "shardId-000000000003"
          }
        ],
        "StreamStatus" => Keyword.get(opts, :stream_status, "ACTIVE")
      }
    }
  end

  def shard_listing do
    [
      %{
        "HashKeyRange" => %{
          "EndingHashKey" => "113427455640312821154458202477256070483",
          "StartingHashKey" => "56713727820156410577229101238628035242"
        },
        "SequenceNumberRange" => %{
          "StartingSequenceNumber" => "49606474897925971126165573671785210788527206059164565522"
        },
        "ShardId" => "shardId-000000000001"
      },
      %{
        "HashKeyRange" => %{
          "EndingHashKey" => "170141183460469231731687303715884105725",
          "StartingHashKey" => "113427455640312821154458202477256070484"
        },
        "SequenceNumberRange" => %{
          "StartingSequenceNumber" => "49606474897948271871364104294926746506799854420670545954"
        },
        "ShardId" => "shardId-000000000002"
      },
      %{
        "HashKeyRange" => %{
          "EndingHashKey" => "226854911280625642308916404954512140967",
          "StartingHashKey" => "170141183460469231731687303715884105726"
        },
        "SequenceNumberRange" => %{
          "StartingSequenceNumber" => "49606474897970572616562634918068282225072502782176526386"
        },
        "ShardId" => "shardId-000000000003"
      },
      %{
        "HashKeyRange" => %{
          "EndingHashKey" => "283568639100782052886145506193140176209",
          "StartingHashKey" => "226854911280625642308916404954512140968"
        },
        "SequenceNumberRange" => %{
          "StartingSequenceNumber" => "49606474897992873361761165541209817943345151143682506818"
        },
        "ShardId" => "shardId-000000000004"
      },
      %{
        "HashKeyRange" => %{
          "EndingHashKey" => "340282366920938463463374607431768211455",
          "StartingHashKey" => "283568639100782052886145506193140176210"
        },
        "SequenceNumberRange" => %{
          "StartingSequenceNumber" => "49606474898015174106959696164351353661617799505188487250"
        },
        "ShardId" => "shardId-000000000005"
      }
    ]
  end
end
