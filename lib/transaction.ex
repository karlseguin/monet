defmodule Monet.Transaction do
	@moduledoc """
	Created via `Monet.transaction/1` or `Monet.transaction/2`.
	"""

	alias __MODULE__
	alias Monet.{Connection, Error, Reader, Writer}

	# use a struct instead of a record so that we can pattern match it easier
	defstruct [:conn]

	def new(conn), do: %Transaction{conn: conn}

	@doc """
	Executes the query using the specific transaction
	"""
	def query(tx, sql, args \\ nil)

	# With no arguments, we can pass this to `Connection.query/3` as-is
	def query(tx, sql, args) do
		# Connection.query returns {result, conn} for the pool
		# we only care about the result
		tx.conn |> Connection.query(sql, args) |> elem(0)
	end

	def commit(tx) do
		conn = tx.conn
		with :ok, Writer.query(conn, "commit"),
		     {:ok, "&4 t\n"} <- Reader.message(conn)
		do
			:ok
		else
			{:ok, data} -> {:error, Error.new(:driver, "invalid commit response", data)}
			err -> err
		end
	end

	def rollback(tx) do
		conn = tx.conn
		with :ok, Writer.query(conn, "rollback"),
		     {:ok, "&4 t\n"} <- Reader.message(conn)
		do
			:ok
		else
			{:ok, data} -> {:error, Error.new(:driver, "invalid rollback response", data)}
			err -> err
		end
	end

end
