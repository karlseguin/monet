defmodule Monet do
	@moduledoc """
	Main interface for interacting with a MonetDB server. Implemented as a pool
	of Monet.Connection. Commands such as query/1 check out a Connection from
	the pool, call query/1 on it, and then check it back in.

	The pool has a simple backoff implementation in the case of failed connections.
	The maximum sleep time before trying to reconnect is 4 seconds.
	"""

	require Record

	@behaviour NimblePool

	Record.defrecord(:pool,
		config: nil,
		failures: 0
	)

	alias Monet.{Connection, Result, Transaction}

	@doc """
	Returns a supervisor child specification for a pool of Monet.Connections.
	"""
	def child_spec(opts) do
		%{
			id: opts[:name] || __MODULE__,
			start: {Monet, :start_link, [opts]}
		}
	end

	@doc """
	Starts the pool and establishes the specified number of connections

	## Options
	* `:connect_timeout` - Timeout, in milliseconds, to conn10_000
	* `:database` - Database to conenct to (default: "monetdb")
	* `:host` - Hostname or IP of the server to connect to (default:"127.0.0.1")
	* `:name` - The process name of the pool (default: Monet)
	* `:password` - Password to use (default: "monetdb")
	* `:pool_size` - Size of the conncetion pool (default: 10)
	* `:port` - Port of the server to connect to (default: 50_000)
	* `:read_timeout` - Timeout, in milliseconds, for individual tcp recv operations (default: 10_000)
	* `:send_timeout` - Timeout, in milliseconds, for individual tcp send operatins (default: 10_000)
	* `:username` - Username to use (default: "monetdb")

	If a `:name` is given, that value must be provided as the first parameter to
	other functions in this module.
	"""
	def start_link(opts) do
		{name, opts} = case Keyword.get(opts, :name) do
			nil -> {__MODULE__, Keyword.put(opts, :name, __MODULE__)}
			name -> {name, opts}
		end
		{pool_size, opts} = Keyword.pop(opts, :pool_size, 10)
		p = pool(failures: 0, config: opts)
		child = {NimblePool, worker: {Monet, p}, pool_size: pool_size, name: name}
		Supervisor.start_link([child], strategy: :one_for_one)
	end

	@doc """
	Executes a query. Returns {:ok, %Monet.Result{}} or {:error, %Monet.Error{}}
	If no pool name is given, then the default name, Monet, is used.
	If no arguments are given, then a simple query is executed, otherwise, the
	query is prepared + executed + deallocated.
	"""
	def query(sql), do: query(__MODULE__, sql, nil)
	def query(pool, sql) when is_atom(pool) or is_pid(pool), do: query(pool, sql, nil)

	def query(tx, sql) when elem(tx, 0) == :transaction, do: query(tx, sql, nil)
	def query(sql, args), do: query(__MODULE__, sql, args)

	def query(tx, sql, args) when elem(tx, 0) == :transaction, do: Transaction.query(tx, sql, args)

	def query(pool, sql, args) do
		NimblePool.checkout!(pool, :checkout, fn _from, conn ->
			Connection.query(conn, sql, args)
		end)
	end

	@doc """
	Same as query but returns a %Monet.Result{} or raises a %Monet.Error{}.
	"""
	def query!(sql), do: query!(__MODULE__, sql, nil)
	def query!(pool, sql) when is_atom(pool) or is_pid(pool), do: query!(pool, sql, nil)
	def query!(sql, args), do: query!(__MODULE__, sql, args)
	def query!(pool, sql, args) do
		case query(pool, sql, args) do
			{:ok, result} -> result
			{:error, err} -> raise err
		end
	end

	@doc """
	Checkouts a connection and runs `fun` within a transaction.

	If `fun` returns `{:rollback, res}`, the transaction is rolled back and
	`{:error, res}` is returned. Alternatively, `Monet.rollback/2` can be used to
	the same effect.

	NOTE: The above means that, unlike other functions which only return
	`{:ok, %Monet.Result{}}` or `{:error, %Monet.Error{}}`, this function can
	also return `{:error, term}` where `term` is any value passed to `Monet.rollback/2`)
	or specified via `{:rollback, value}`.

	Any other value returned by `fun` will result in the transaction being committed:

			Monet.transaction(fn tx ->
				Monet.query(tx, "....")
				Monet.query(tx, "....")
			end)

	To be more explicit, `fun` can return `{:commit, result}` in which case the
	transaction will be commited and `{:ok, result}` will be returned.
	"""
	def transaction(pool \\ __MODULE__, fun) do
		NimblePool.checkout!(pool, :checkout, fn _from, conn ->
			Connection.transaction(conn, fun)
		end)
	end

	@doc """
	Same as query but returns a %Monet.Result{}, raises a %Monet.Error{} or raises
	whatever custom rollback value you provide.
	"""
	def transaction!(pool \\ __MODULE__, sql) do
		case transaction(pool, sql) do
			{:ok, result} -> result
			{:error, err} -> raise err
		end
	end

	@doc """
	Commits the transaction. This can either be called implicitly based on the
	return value of your transaction `fun`:

			Monet.transaction!(fn tx ->
				....
				{:commit, return_value} # or simply {:ok, return_value}
			end)

	 or explicitly:

			Monet.transaction!(fn tx ->
				....
				Monet.commit(tx, return_value)
			end)
	"""
	def commit(tx, {:ok, result}), do: commit(tx, result)
	def commit(tx, {:commit, result}), do: commit(tx, result)
	def commit(tx, result) do
		with :ok <- Monet.Transaction.commit(tx) do
			{:ok, result}
		end
	end

	@doc """
	Rollsback the transaction. This can either be called implicitly based on the
	return value of your transaction `fun`:

			Monet.transaction!(fn tx ->
				....
				{:rollback, return_value}
			end)

	 or explicitly:

			Monet.transaction!(fn tx ->
				....
				Monet.rollback(tx, return_value)
			end)
	"""
	def rollback(tx, result) do
		with :ok <- Monet.Transaction.rollback(tx) do
			{:error, result}
		end
	end

	@doc """
	Creates a prepared statement for use in a transaction. The statements are
	automatically cleaned up at the end of the transaction.

	Use with care. MonetDB automatically deallocates prepared statements on
	execution error. If a query using a prepared statement fails in your transaction
	you should probably end the transaction.

			Monet.transaction(fn tx ->
				Monet.prepare(tx, :test_insert, "insert into test (id) values (?)")
				with {:ok, r1} <- Monet.query(tx, :test_insert, [1]),
				     {:ok, r2} <- Monet.query(tx, :test_insert, [2])
				do
					{:ok, [r1, r2]}
				else
					err -> {:rollback, err}
				end
			end)
	"""
	def prepare(tx, name, sql) when elem(tx, 0) == :transaction do
		Transaction.prepare(tx, name, sql)
	end

	def prepare!(tx, name, sql) when elem(tx, 0) == :transaction do
		case prepare(tx, name, sql) do
			:ok -> :ok
			{:error, err} -> raise err
		end
	end

	@doc """
	By default, the `Monet.Result` returned from a select will enumerate a list of
	lists (via the Enumerable protocol or Jason.encode).

	The behavior can be changed to enumerate or encode to a list of maps:

			"select id, name from saiyans"
			|> Monet.query!()
			|> Monet.as_map()
			|> Jason.encode()  # or Enum.reduce/map/...

	Note that calling `as_map` does not change the `rows` field of the result. It
	merely configures how the enumeration will operate. So, to get a list of all
	maps with columns as atoms, one would do:

			"select id, name from saiyans"
			|> Monet.query!()
			|> Monet.as_map(columns: :atoms)
			|> Enum.list()

	`as_map` works on any value returned by `query` and `query!`. In the case of an
	error, the error is simply returned. As such, a safer usage would be:

		case Monet.at_map(Monet.query("select id, name from saiyans")) do
			{:ok, result} -> ...
			{:error, err} -> ...
		end
	"""
	def as_map(value, opts \\ [])
	def as_map({:ok, %Result{} = result}, opts), do: as_map(result, opts)
	def as_map(%Result{} = result, opts), do: Result.as_map(result, opts)
	def as_map(error, _opts), do: error

	@impl NimblePool
	def init_pool(state) do
		name = Keyword.fetch!(pool(state, :config), :name)
		:ets.new(name, [:set, :public, :named_table])
		{:ok, state}
	end

	@impl NimblePool
	def init_worker(state) do
		with {:ok, conn} <- Connection.connect(pool(state, :config)),
				 :ok <- Connection.controlling_process(conn, self())
		do
			{:ok, conn, reset(state)}
		else
			_ -> {:ok, nil, backoff(state)}
		end

	end

	@impl NimblePool
	def handle_checkout(:checkout, _, nil, state)  do
		failures = pool(state, :failures)
		# micro-opt, bu thtere's no point in increasing this value any more since
		# we've reached our max backoff
		state = case failures > 10 do
			true -> state
			false -> pool(state, failures: failures + 1)
		end
		{:remove, :down, state}
	end

	def handle_checkout(:checkout, {_pid, _}, conn, state) do
		{:ok, conn, conn, state}
	end

	@impl NimblePool
	def terminate_worker(_reason, conn, pool_state) do
		Connection.close(conn)
		{:ok, pool_state}
	end

	# Once we've successfully connected, we want to reset the pool's failure count
	defp reset(state) do
		case pool(state, :failures) do
			0 -> state
			_ -> pool(state, failures: 0)
		end
	end

	defp backoff(state) do
		failures = pool(state, :failures)
		case failures do
			0 -> :ok
			1 -> :ok
			2 -> :timer.sleep(100)
			3 -> :timer.sleep(300)
			4 -> :timer.sleep(600)
			5 -> :timer.sleep(1000)
			6 -> :timer.sleep(2000)
			7 -> :timer.sleep(3000)
			_ -> :timer.sleep(4000)
		end
		pool(state, failures: failures + 1)
	end
end
