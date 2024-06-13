defmodule HammerBackendRedisTest do
  use ExUnit.Case

  alias Hammer.Backend
  alias Hammer.Utils

  setup do
    {Backend.Redis, config} = Application.get_env(:hammer, :backend)
    {:ok, pid} = Backend.Redis.start_link(config)
    %{redix: redix} = :sys.get_state(pid)

    assert {:ok, "OK"} = Redix.command(redix, ["FLUSHALL"])
    {:ok, [pid: pid, redix: redix, key_prefix: Keyword.get(config, :key_prefix, "Hammer:Redis:")]}
  end

  test "count_hit, insert", %{pid: pid, redix: redix, key_prefix: key_prefix} do
    bucket = 1
    id = "one"
    bucket_key = {bucket, id}
    scale_seconds = 20
    scale_ms = :timer.seconds(scale_seconds)
    redis_key = make_redis_key(key_prefix, bucket_key)

    assert {:ok, 0} = Redix.command(redix, ["EXISTS", redis_key])
    assert {:ok, 1, created_at} = Backend.Redis.count_hit(pid, bucket_key, scale_ms)
    assert {:ok, 1} = Redix.command(redix, ["EXISTS", redis_key])

    created_at_str = to_string(created_at)

    assert {:ok, ["count", "1", "created", ^created_at_str, "updated", ^created_at_str]} =
             Redix.command(redix, ["HGETALL", redis_key])

    assert {:ok, scale_seconds} == Redix.command(redix, ["TTL", redis_key])
  end

  test "count_hit, insert, with custom increment", %{
    pid: pid,
    redix: redix,
    key_prefix: key_prefix
  } do
    bucket = 1
    id = "one"
    bucket_key = {bucket, id}
    inc = Enum.random(1..100)
    inc_str = Integer.to_string(inc)
    scale_seconds = 20
    scale_ms = :timer.seconds(scale_seconds)
    redis_key = make_redis_key(key_prefix, bucket_key)

    assert {:ok, ^inc, created} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, inc)
    assert {:ok, 1} = Redix.command(redix, ["EXISTS", redis_key])

    created_str = to_string(created)

    assert {:ok, ["count", ^inc_str, "created", ^created_str, "updated", ^created_str]} =
             Redix.command(redix, ["HGETALL", redis_key])

    assert {:ok, scale_seconds} == Redix.command(redix, ["TTL", redis_key])
  end

  test "count_hit, update", %{pid: pid, redix: redix, key_prefix: key_prefix} do
    # 1. set-up
    bucket = 1
    id = "one"
    bucket_key = {bucket, id}
    scale_seconds = 20
    scale_ms = :timer.seconds(scale_seconds)
    redis_key = make_redis_key(key_prefix, bucket_key)

    assert {:ok, 1, created_at} = Backend.Redis.count_hit(pid, bucket_key, scale_ms)
    assert {:ok, 1} = Redix.command(redix, ["EXISTS", redis_key])

    assert {:ok, scale_seconds} == Redix.command(redix, ["TTL", redis_key])

    created_at_str = to_string(created_at)

    sleep_seconds = 1
    :timer.sleep(sleep_seconds * 1000)

    # 2. function call under test: count == 2
    assert {:ok, 2, created_at} == Backend.Redis.count_hit(pid, bucket_key, scale_ms)
    assert {:ok, 1} = Redix.command(redix, ["EXISTS", redis_key])

    assert {:ok, ["count", "2", "created", ^created_at_str, "updated", updated_str]} =
             Redix.command(redix, ["HGETALL", redis_key])

    {updated_at, ""} = Integer.parse(updated_str)

    assert updated_at >= created_at

    assert {:ok, new_expiry_seconds} = Redix.command(redix, ["TTL", redis_key])

    # expiry should NOT be reset if it has already been set before
    assert new_expiry_seconds > scale_seconds - sleep_seconds - 0.1
    assert new_expiry_seconds <= scale_seconds - sleep_seconds
  end

  test "get_bucket", %{pid: pid} do
    # 1. set-up
    bucket = 1
    id = "one"
    bucket_key = {bucket, id}
    scale_ms = :timer.seconds(20)

    inc_before = Enum.random(1..100)

    inc_after = Enum.random(1..100)

    inc_total = inc_before + inc_after

    assert {:ok, ^inc_before, created_at1} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, inc_before)

    assert created_at1 >= Utils.timestamp()

    assert {:ok, ^inc_total, created_at2} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, inc_after)

    assert created_at1 == created_at2

    # 2. function call under test
    assert {:ok, {^bucket_key, ^inc_total, ^created_at1, ^created_at1}} =
             Backend.Redis.get_bucket(pid, bucket_key)
  end

  test "delete buckets", %{pid: pid, redix: redix} do
    bucket = 1
    id = "one"
    bucket_key = {bucket, id}
    scale_ms = :timer.seconds(20)

    {:ok, _, _} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, 1)
    assert {:ok, 1} = Backend.Redis.delete_buckets(pid, id)
    assert {:ok, []} = Redix.command(redix, ["KEYS", "*"])
  end

  test "delete buckets with no keys matching", %{pid: pid, redix: redix} do
    id = "one"
    scale_ms = :timer.seconds(20)

    for bucket <- 1..10 do
      bucket_key = {bucket, id}
      {:ok, _, _} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, 1)
    end

    assert {:ok, 0} = Backend.Redis.delete_buckets(pid, "foobar")

    # Previous keys remain untouched
    {:ok, keys} = Redix.command(redix, ["KEYS", "*"])
    assert 10 == length(keys)
  end

  test "delete buckets when many buckets exist", %{pid: pid, redix: redix} do
    id = "one"
    scale_ms = :timer.seconds(20)

    for bucket <- 1..1000 do
      bucket_key = {bucket, id}
      {:ok, _, _} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, 1)
    end

    assert {:ok, 1_000} = Backend.Redis.delete_buckets(pid, id)
    assert {:ok, []} = Redix.command(redix, ["KEYS", "*"])
  end

  test "delete buckets when scan returns empty list but valid cursor", %{pid: pid, redix: redix} do
    # By writing only one key to the keyspace, most iterations of the SCAN call
    # will return a valid cursor but an empty list. The valid cursor should
    # make the delete_buckets call iterate until the intended key is found
    id = "one"
    bucket_key = {1, id}
    scale_ms = :timer.seconds(20)
    {:ok, _, _} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, 1)

    another_id = "another"

    for bucket <- 1..1000 do
      bucket_key = {bucket, another_id}
      {:ok, _, _} = Backend.Redis.count_hit(pid, bucket_key, scale_ms, 1)
    end

    assert {:ok, 1} = Backend.Redis.delete_buckets(pid, id)
    {:ok, keys} = Redix.command(redix, ["KEYS", "*"])
    assert 1_000 == length(keys)
  end

  defp make_redis_key(prefix, {bucket, id}) do
    "#{prefix}#{id}:#{bucket}"
  end
end
