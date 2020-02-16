defmodule KinesisClient.Stream.AppState.ShardLeaseTest do
  use KinesisClient.Case

  alias KinesisClient.Stream.AppState.ShardLease
  alias ExAws.Dynamo.Encodable

  test "ExAws.Dynamo.Encodable.encode/2 implemented" do
    result = Encodable.encode(%ShardLease{}, [])

    assert result == %{
             "M" => %{
               "checkpoint" => %{"NULL" => "true"},
               "completed" => %{"BOOL" => "false"},
               "lease_count" => %{"NULL" => "true"},
               "lease_owner" => %{"NULL" => "true"},
               "shard_id" => %{"NULL" => "true"}
             }
           }
  end
end
