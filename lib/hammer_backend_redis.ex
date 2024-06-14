defmodule Hammer.Backend.Redis do
  @moduledoc """
  Documentation for Hammer.Backend.Redis

  This backend uses the [Redix](https://hex.pm/packages/redix) library to connect to Redis.

  The backend process is started by calling `start_link`:

      Hammer.Backend.Redis.start_link(
        expiry_ms: 60_000 * 10,
        redix_config: [host: "example.com", port: 5050]
      )

  Options are:

  - `expiry_ms`: Expiry time of buckets in milliseconds,
    used to set TTL on Redis keys. This configuration is mandatory.
  - `redix_config`: Keyword list of options to the `Redix` redis client,
    also aliased to `redis_config`
  - `key_prefix`: The prefix to use for all the redis keys (defaults to "Hammer:Redis:")
  - `redis_url`: String url of redis server to connect to
    (optional, invokes Redix.start_link/2)
  """

  @behaviour Hammer.Backend

  use GenServer

  alias Hammer.Utils

  @default_timeout :timer.seconds(5)

  @type bucket_key :: {bucket :: integer, id :: String.t()}
  @type bucket_info ::
          {key :: bucket_key, count :: integer, created :: integer, updated :: integer}

  ## Public API

  @spec start :: :ignore | {:error, any} | {:ok, pid}
  def start do
    start([])
  end

  @spec start(keyword()) :: :ignore | {:error, any} | {:ok, pid}
  def start(args) do
    GenServer.start(__MODULE__, args)
  end

  @spec start_link :: :ignore | {:error, any} | {:ok, pid}
  def start_link do
    start_link([])
  end

  @spec start_link(keyword()) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @spec stop :: any
  def stop do
    GenServer.call(__MODULE__, :stop, call_timeout())
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
  end

  @doc """
  Record a hit in the bucket identified by `key`
  """
  @impl Hammer.Backend
  def count_hit(pid, key, scale_ms) do
    GenServer.call(pid, {:count_hit, key, scale_ms, 1}, call_timeout())
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
  end

  @doc """
  Record a hit in the bucket identified by `key`, with a custom increment
  """
  @impl Hammer.Backend
  def count_hit(pid, key, scale_ms, increment) do
    GenServer.call(pid, {:count_hit, key, scale_ms, increment}, call_timeout())
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
  end

  @doc """
  Retrieve information about the bucket identified by `key`
  """
  @impl Hammer.Backend
  def get_bucket(pid, key) do
    GenServer.call(pid, {:get_bucket, key}, call_timeout())
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
  end

  @doc """
  Delete all buckets associated with `id`.
  """
  @impl Hammer.Backend
  def delete_buckets(pid, id) do
    delete_buckets_timeout = GenServer.call(pid, {:get_delete_buckets_timeout}, call_timeout())
    GenServer.call(pid, {:delete_buckets, id}, delete_buckets_timeout)
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
  end

  ## GenServer Callbacks

  @impl GenServer
  def init(args) do
    expiry_ms = Keyword.get(args, :expiry_ms)

    if !expiry_ms do
      raise RuntimeError, "Missing required config: expiry_ms"
    end

    key_prefix = Keyword.get(args, :key_prefix, "Hammer:Redis:")

    redix_config =
      Keyword.get(
        args,
        :redix_config,
        Keyword.get(args, :redis_config, [])
      )

    redis_url = Keyword.get(args, :redis_url, nil)

    {:ok, redix} =
      if is_binary(redis_url) && byte_size(redis_url) > 0 do
        Redix.start_link(redis_url, redix_config)
      else
        Redix.start_link(redix_config)
      end

    delete_buckets_timeout = Keyword.get(args, :delete_buckets_timeout, @default_timeout)

    {:ok,
     %{
       redix: redix,
       expiry_ms: expiry_ms,
       delete_buckets_timeout: delete_buckets_timeout,
       key_prefix: key_prefix
     }}
  end

  @impl GenServer
  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:count_hit, key, scale_ms, increment}, _from, state) do
    global_expiry_seconds = get_global_expiry_in_seconds(state)
    bucket_timespan_seconds = milliseconds_to_seconds(scale_ms)
    expiry = min(global_expiry_seconds, bucket_timespan_seconds)

    result = do_count_hit(state, key, increment, expiry)
    {:reply, result, state}
  end

  def handle_call({:get_bucket, key}, _from, %{redix: r} = state) do
    redis_key = make_redis_key(state, key)
    command = ["HMGET", redis_key, "count", "created", "updated"]

    result =
      case Redix.command(r, command) do
        {:ok, [nil, nil, nil]} ->
          {:ok, nil}

        {:ok, [count, created, updated]} ->
          count = String.to_integer(count)
          created = String.to_integer(created)
          updated = String.to_integer(updated)
          {:ok, {key, count, created, updated}}

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, result, state}
  end

  def handle_call({:delete_buckets, id}, _from, %{redix: r} = state) do
    redis_key_pattern = make_redis_key_pattern(state, id)
    result = do_delete_buckets(r, redis_key_pattern, 0, 0)

    {:reply, result, state}
  end

  def handle_call({:get_delete_buckets_timeout}, _from, %{delete_buckets_timeout: delete_buckets_timeout} = state) do
    {:reply, delete_buckets_timeout, state}
  end

  defp do_delete_buckets(r, redis_key_pattern, cursor, count_deleted) do
    case Redix.command(r, ["SCAN", cursor, "MATCH", redis_key_pattern]) do
      {:ok, ["0", []]} ->
        {:ok, count_deleted}

      {:ok, [next_cursor, []]} ->
        do_delete_buckets(r, redis_key_pattern, next_cursor, count_deleted)

      {:ok, ["0", keys]} ->
        {:ok, deleted} = Redix.command(r, ["DEL" | keys])
        {:ok, deleted + count_deleted}

      {:ok, [next_cursor, keys]} ->
        {:ok, deleted} = Redix.command(r, ["DEL" | keys])
        do_delete_buckets(r, redis_key_pattern, next_cursor, count_deleted + deleted)

      {:error, reason} ->
        {:error, reason}
    end
  end

  # we are using the first method described (called bucketing)
  # in https://www.youtube.com/watch?v=CRGPbCbRTHA
  # but we add the 'created' and 'updated' meta information fields.
  defp do_count_hit(%{redix: r} = state, key, increment, expiry) do
    redis_key = make_redis_key(state, key)
    now = Utils.timestamp()

    cmds = [
      ["MULTI"],
      [
        "HINCRBY",
        redis_key,
        "count",
        increment
      ],
      [
        "HSETNX",
        redis_key,
        "created",
        now
      ],
      [
        "HSET",
        redis_key,
        "updated",
        now
      ],
      [
        "EXPIRE",
        redis_key,
        expiry,
        "NX"
      ],
      [
        "HMGET",
        redis_key,
        "created"
      ],
      ["EXEC"]
    ]

    case Redix.pipeline(r, cmds) do
      {:ok, ["OK", "QUEUED", "QUEUED", "QUEUED", "QUEUED", "QUEUED", [new_count, _, _, _, [created]]]} ->
        created = String.to_integer(created)
        {:ok, new_count, created}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp make_redis_key(%{key_prefix: key_prefix}, {bucket, id}) do
    "#{key_prefix}#{id}:#{bucket}"
  end

  defp make_redis_key_pattern(%{key_prefix: key_prefix}, id) do
    "#{key_prefix}#{id}:*"
  end

  defp get_global_expiry_in_seconds(state) do
    %{expiry_ms: expiry_ms} = state
    milliseconds_to_seconds(expiry_ms) + 1
  end

  defp milliseconds_to_seconds(milliseconds) do
    round(milliseconds / 1000)
  end

  defp call_timeout do
    Application.get_env(:hammer, :gen_server_call_timeout, @default_timeout)
  end
end
