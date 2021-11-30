defmodule KinesisClient.Stream.AppState.DynamoTest do
  use KinesisClient.Case

  alias KinesisClient.Stream.AppState.Dynamo, as: AppState
  alias KinesisClient.Stream.AppState.ShardLease
  alias ExAws.Dynamo

  setup_all do
    app_name = "foo_app_#{random_string()}"

    {:ok, _result} =
      Dynamo.create_table(app_name, "shard_id", %{shard_id: :string}, 1, 1) |> ExAws.request()

    :ok = confirm_table_created(app_name)
    %{app_name: app_name}
  end

  describe "initialize/2" do
    test "creates a new table successfully" do
      app_name = "foo_app_#{random_string()}"

      assert :ok = AppState.initialize(app_name, [])
    end

    test "passes if table already created", %{app_name: app_name} do
      assert :ok = AppState.initialize(app_name, [])
    end
  end

  describe "create_lease/3" do
    test "lease created and :ok returned if no record for shard exists", %{app_name: app_name} do
      result = AppState.create_lease(app_name, random_string(), worker_ref(), [])
      assert result == :ok
    end

    test "returns :already_exists if record for shard exists", %{app_name: app_name} do
      shard_id = random_string()
      lease_owner = worker_ref()
      assert :ok == AppState.create_lease(app_name, shard_id, lease_owner, [])
      assert :already_exists == AppState.create_lease(app_name, shard_id, lease_owner, [])
    end
  end

  test "get_lease/3 is successful", %{app_name: app_name} do
    shard_id = random_string()
    worker_ref = worker_ref()
    result = AppState.create_lease(app_name, shard_id, worker_ref, [])
    assert result == :ok

    %ShardLease{} = AppState.get_lease(app_name, shard_id, [])
  end

  describe "renew_lease/3" do
    test "successfully increments lease_count", %{app_name: app_name} do
      shard_id = random_string()
      worker_ref = worker_ref()
      result = AppState.create_lease(app_name, shard_id, worker_ref, [])
      assert result == :ok

      shard_lease = %ShardLease{lease_count: 1, shard_id: shard_id, lease_owner: worker_ref}
      assert {:ok, result} = AppState.renew_lease(app_name, shard_lease, [])

      assert result == shard_lease.lease_count + 1
    end

    test "returns error if lease_owner does not match input lease_owner", %{app_name: app_name} do
      shard_id = random_string()
      lease_owner = worker_ref()
      result = AppState.create_lease(app_name, shard_id, lease_owner, [])
      assert result == :ok

      shard_lease = %ShardLease{lease_count: 1, shard_id: shard_id, lease_owner: worker_ref()}
      assert {:error, :lease_renew_failed} = AppState.renew_lease(app_name, shard_lease, [])
    end

    test "returns error if lease_count does not match input lease_count", %{app_name: app_name} do
      shard_id = random_string()
      lease_owner = worker_ref()
      result = AppState.create_lease(app_name, shard_id, lease_owner, [])
      assert result == :ok

      shard_lease = %ShardLease{lease_count: 2, shard_id: shard_id, lease_owner: lease_owner}
      assert {:error, :lease_renew_failed} = AppState.renew_lease(app_name, shard_lease, [])
    end
  end

  describe "take_lease/3" do
    test "successful if lease_count equals current lease count", %{app_name: app_name} do
      shard_id = random_string()
      old_lease_owner = worker_ref()
      new_lease_owner = worker_ref()
      result = AppState.create_lease(app_name, shard_id, old_lease_owner, [])
      assert result == :ok

      assert {:ok, result} = AppState.take_lease(app_name, shard_id, new_lease_owner, 1, [])

      assert result == 2
    end

    test "unsuccessful if lease_count not equal to current lease count", %{app_name: app_name} do
      shard_id = random_string()
      old_lease_owner = worker_ref()
      new_lease_owner = worker_ref()
      result = AppState.create_lease(app_name, shard_id, old_lease_owner, [])
      assert result == :ok

      assert {:error, :lease_take_failed} =
               AppState.take_lease(app_name, shard_id, new_lease_owner, 3, [])
    end

    test "unsuccessful if new_lease_owner equals current lease ownere", %{app_name: app_name} do
      shard_id = random_string()
      lease_owner = worker_ref()
      result = AppState.create_lease(app_name, shard_id, lease_owner, [])
      assert result == :ok

      assert {:error, :lease_take_failed} =
               AppState.take_lease(app_name, shard_id, lease_owner, 1, [])
    end
  end

  describe "update_checkpoint/5" do
    test "success", %{app_name: app_name} do
      shard_id = random_string()
      lease_owner = worker_ref()
      checkpoint = "239801209190"

      result = AppState.create_lease(app_name, shard_id, lease_owner, [])
      assert result == :ok

      assert :ok = AppState.update_checkpoint(app_name, shard_id, lease_owner, checkpoint, [])
    end
  end

  describe "close_shard/4" do
    test "success", %{app_name: app_name} do
      shard_id = random_string()
      lease_owner = worker_ref()

      result = AppState.create_lease(app_name, shard_id, lease_owner, [])
      assert result == :ok

      assert :ok = AppState.close_shard(app_name, shard_id, lease_owner, [])
    end
  end

  defp confirm_table_created(app_name, attempts \\ 1) do
    case Dynamo.describe_table(app_name) |> ExAws.request() do
      {:ok, %{"Table" => %{"TableStatus" => "CREATING"}}} ->
        case attempts do
          x when x <= 5 -> confirm_table_created(app_name, attempts + 1)
          _ -> raise "could not create dynamodb table!"
        end

      {:ok, _x} ->
        :ok

      {:error, e} ->
        case attempts do
          x when x <= 5 -> confirm_table_created(app_name, attempts + 1)
          _ -> raise "could not create dynamodb table! #{inspect(e)}"
        end
    end
  end
end
