defmodule Monet.Connection do
	@moduledoc """
	Represents a connection (a socket) to the MonetDB Server.

	Although this can be accessed directy (staring with conn/1), the intention is
	for it to be accessed via the Monet module (which manages a pool of these
	connections).
	"""

	require Record
	require Logger

	alias Monet.{Error, Prepared, Reader, Transaction, Writer}

	Record.defrecord(:connection,
		socket: nil,
		pool_name: nil,
		read_timeout: 10_000,
		send_timeout: 10_000,
		connect_timeout: 10_000
	)

	@doc """
	A query with no arguments is executed as a simple query. A query with arguments
	is executed as a prepare + exec + deallocate.

	Query does not mutate the conn, but conn is returned nonetheless (along with the
	result or error). The returned conn can be nil, which indicates that the connection
	can no longer be used.
	"""
	def query(conn, sql, args \\ nil)

	def query(conn, sql, nil) do
		with :ok <- Writer.query(conn, sql),
		     {:ok, } = result <- Reader.result(conn)
		do
			{result, conn}
		else
			err -> error_result(err, conn)
		end
	end

	def query(conn, sql, args) do
		with {:ok, prepared} <- Prepared.new(conn, sql),
		     {:ok, result, c} <- Prepared.exec_and_close(prepared, args)
		do
			case c do
				:ok -> {result, conn}
				{:error, %{code: 7003}} ->
					# Deallocating failed because the id wasn't valid. This easily happens
					# (monetd automatically deallocates on a failed execution). It's no
					# reason to remove the connection from the pool.
					{result, conn}
				_ ->
					# We got an error deallocating which wasn't specifically about an invalid
					# id. The connection is probably still good, but we don't want to leak
					# prepared stamenents on the server. Safer to close this connection to
					# force a cleanup.
					{result, close(conn)}
			end
		else
			err -> error_result(err, conn)
		end
	end

	@doc """
	Runs `fun` in a transaction. Automaticaly starts and commits/rollsback the
	transaction.

	When called through the pool (that is via `Monet.transaction/1`) the connection
	is automatically closed (and thus the transaction rolledback) on an exception.

	If calling this directly, it is up to the caller to deal with exceptions.
	"""
	def transaction(conn, fun) do
		tx = Transaction.new(conn)
		with :ok <- Writer.query(conn, "start transaction"),
		     {:ok, _} <- Reader.result(conn)
		do
			try do
				fun_result = fun.(tx)

				# commit/rollback result
				crb_result = case fun_result do
					{:rollback, _} -> Transaction.rollback(tx)
					_ -> Transaction.commit(tx)
				end

				case crb_result do
					# If the commit/rollback failed, this is the result we'll return
					# plus we might need to close the connection
					{:error, _} -> {crb_result, close(conn)}
					:ok ->
						# If the commit/rollback succeeded, we need to clean up the value
						# returned by the supplied fun and we know we don't need to close
						# the connection
						value = case fun_result do
							{:ok, _} = ok -> ok
							{:error, _} = err -> err
							{:rollback, _} = rlb -> rlb
							{:commit, value} -> {:ok, value}
							value -> {:ok, value}
						end
						{value, conn}
				end
			after
				Transaction.close(tx) # deallocate any prepared statements
			end
		else
			err -> error_result(err, conn)
		end
	end

	@doc """
	Connects to the MonetDB server. See Monet.start_link/1 for available options
	(although some of the options listed there such as `pool_size` are
	specific to the Monet pool and not this individual connection).
	"""
	def connect(opts) do
		connect_timeout = Keyword.get(opts, :connect_timeout, 10_000)
		case :gen_tcp.connect(host(opts), port(opts), [packet: :raw, mode: :binary, active: false], connect_timeout) do
			{:ok, socket} ->
				with {:ok, conn} <- authenticate(socket, opts),
			       {:ok, conn} <- configure(conn, opts)
				do
					{:ok, conn}
				else
					{:error, err} = error ->
						:gen_tcp.close(socket)
						Logger.error("Failed to initialie connection - #{inspect(err)}")
						error
				end
			{:error, err} = error ->
				Logger.error("Failed to connect to MonetDB on #{host(opts)}:#{port(opts)} - #{inspect(err)}")
				error
		end
	end

	def close(conn) do
		:gen_tcp.close(connection(conn, :socket))
		nil
	end

	# I don't think this logic is quite right. The idea is that we don't want
	# to return a dead connection back into the pool.
	defp error_result({:error, err} = result, conn) do
		case Error.closed?(err) do
			true -> {result, close(conn)}
			false -> {result, conn}
		end
	end

	defp error_result(result, conn) do
		{result, conn}
	end

	@doc """
	In Elixir, every socket is assigned a "controlling" process. This is to control
	the destination of incoming data when the socket is in active mode. We don't
	use active mode (but we might leverage it in the future).

	Still, it appears that the socket is also tied to the lifetime of the
	controlling process, so we do have to set this once in Monet.init_worker.
	"""
	def controlling_process(conn, pid) do
		socket = connection(conn, :socket)
		case :gen_tcp.controlling_process(socket, pid) do
			:ok -> :ok
			err -> :gen_tcp.close(socket); err
		end
	end

	defp authenticate(socket, opts) do
		pool_name = Keyword.get(opts, :name, Monet)
		send_timeout = Keyword.get(opts, :send_timeout, 10_000)
		read_timeout = Keyword.get(opts, :read_timeout, 10_000)
		connect_timeout = Keyword.get(opts, :connect_timeout, 10_000)

		username = Keyword.get(opts, :username, "monetdb")
		password = Keyword.get(opts, :password, "monetdb")
		database = Keyword.get(opts, :database, "monetdb")

		conn = connection(
			socket: socket,
			pool_name: pool_name,
			send_timeout: send_timeout,
			read_timeout: read_timeout,
			connect_timeout: connect_timeout
		)

		:inet.setopts(socket, send_timeout: Keyword.get(opts, :send_timeout, 10_000))

		case Monet.Auth.login(conn, username: username, password: password, database: database) do
			{:ok, _} = ok -> ok
			{:error, _} = err -> err
			{:redirect, redirect} ->
				:gen_tcp.close(socket)
				connect(Keyword.merge(opts, redirect))
		end
	end

	# there are some commands we want to send on startup
	defp configure(conn, opts) do
		with {:ok, conn} <- set_time_zone(conn, opts),
		     {:ok, conn} <- set_reply_size(conn),
		     {:ok, conn} <- set_schema(conn, opts),
		     {:ok, conn} <- set_role(conn, opts)
		do
			{:ok, conn}
		end
	end

	defp set_time_zone(conn, opts) do
		offset = case Keyword.get(opts, :time_zone_offset) do
			nil -> "0"
			n when is_integer(n) -> Integer.to_string(n)
			err -> {:error, ":time_zone_offset offset must be nil or an integer, got: #{inspect(err)}"}
		end

		Writer.query(conn, "set time zone interval '#{offset}' minute;")
		case Reader.message(conn) do
			{:ok, <<"&3 ", _::binary>>} -> {:ok, conn}
			{:ok, invalid} -> {:error, "Unexpected reply from set time zone: #{invalid}"}
			err -> err
		end
	end

	# I don't know what the default is, but every other drivers sets this so that
	# queries don't return a limited result.
	defp set_reply_size(conn) do
		Writer.command(conn, "reply_size -1")
		case Reader.message(conn) do
			{:ok, ""} -> {:ok, conn}
			{:ok, invalid} -> {:error, "Unexpected reply from reply_size command: #{invalid}"}
			err -> err
		end
	end

	defp set_schema(conn, opts) do
		case Keyword.get(opts, :schema) do
			nil -> {:ok, conn}
			schema ->
				Writer.query(conn, "set schema #{schema}")
				case Reader.message(conn) do
					{:ok, <<"&3 ", _::binary>>} -> {:ok, conn}
					{:ok, invalid} -> {:error, "Unexpected reply from set schema: #{invalid}"}
					err -> err
				end
		end
	end

	defp set_role(conn, opts) do
		case Keyword.get(opts, :role) do
			nil -> {:ok, conn}
			role ->
				Writer.query(conn, "set role #{role}")
				case Reader.message(conn) do
					{:ok, <<"&3 ", _::binary>>} -> {:ok, conn}
					{:ok, invalid} -> {:error, "Unexpected reply from set role: #{invalid}"}
					err -> err
				end
		end
	end

	# only extracted so that we can reuse the logic when logging a connection error
	defp port(opts), do: Keyword.get(opts, :port, 50_000)
	defp host(opts) do
		case Keyword.get(opts, :host) do
			nil -> '127.0.0.1'
			host -> String.to_charlist(host)
		end
	end

	@doc """
	Get the name of the pool this connection belongs tos
	"""
	def pool_name(conn), do: connection(conn, :pool_name)

	def checkout(conn) do
		:inet.setopts(connection(conn, :socket), active: false)
	end

	# set in active so we might be able to detect closed sockets and proactively
	# remove from the pool
	def checkin(conn) do
		:inet.setopts(connection(conn, :socket), active: :once)
	end
end
