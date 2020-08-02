defmodule Monet.Transaction do
	@moduledoc """
	Created via `Monet.transaction/1` or `Monet.transaction/2`.
	"""

	require Record

	alias Monet.{Connection, Error, Prepared, Reader, Writer}

	Record.defrecord(:transaction, conn: nil, ref: nil, pool_name: nil)

	def new(conn) do
		transaction(
			conn: conn,
			ref: make_ref(),
			pool_name: Connection.pool_name(conn)
		)
	end

	@doc """
	Executes the query using the specific transaction
	"""
	def query(tx, name_or_sql, args \\ nil)

	def query(tx, name, args) when is_atom(name) do
		ref = transaction(tx, :ref)
		pool_name = transaction(tx, :pool_name)

		case :ets.lookup(pool_name, {ref, name}) do
			[{_, prepared}] -> Prepared.exec(prepared, args)
			_ -> {:error, Error.new(:driver, "unknown prepared statement", name)}
 		end
	end

	def query(tx, sql, args) do
		# Connection.query returns {result, conn} for the pool
		# we only care about the result
		tx
		|> transaction(:conn)
		|> Connection.query(sql, args)
		|> elem(0)
	end

	@doc """
	Commits the transaction
	"""
	def commit(tx) do
		conn = transaction(tx, :conn)
		with :ok, Writer.query(conn, "commit"),
		     {:ok, "&4 t\n"} <- Reader.message(conn) # make sure auto-commit is turned back on
		do
			:ok
		else
			{:ok, data} -> {:error, Error.new(:driver, "invalid commit response", data)}
			err -> err
		end
	end

	@doc """
	Rollsback the transaction
	"""
	def rollback(tx) do
		conn = transaction(tx, :conn)
		with :ok, Writer.query(conn, "rollback"),
		     {:ok, "&4 t\n"} <- Reader.message(conn)
		do
			:ok
		else
			{:ok, data} -> {:error, Error.new(:driver, "invalid rollback response", data)}
			err -> err
		end
	end

	@doc """
	Prepares the statement and stores it in the transaction cache. See
	`Monet.prepare/3`.
	"""
	def prepare(tx, name, sql) do
		with {:ok, prepared} <- Prepared.new(transaction(tx, :conn), sql)
		do
			ref = transaction(tx, :ref)
			pool_name = transaction(tx, :pool_name)
			:ets.insert(pool_name, {{ref, name}, prepared})
			:ok
		end
	end

	@doc """
	Deallocates any prepared statements that were allocated as part of this
	transaction
	"""
	def close(tx) do
		ref = transaction(tx, :ref)
		pool_name = transaction(tx, :pool_name)
		Enum.each(:ets.match(pool_name, {{ref, :_}, :'$1'}), fn [prepared] ->
			Prepared.close(prepared)
		end)
	end

end
