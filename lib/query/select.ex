defmodule Monet.Query.Select do
	@moduledoc """
	A simple query builder.

			rows = Select.new()
			|> Select.columns("u.id, u.name")
			|> Select.from("users u")
			|> Select.join("roles r on u.role_id = r.id")
			|> Select.where("u.power", :gt, 9000)
			|> Select.limit(100)
			|> Select.exec!()  // returns a Monet.Result
			|> Monet.rows()
	"""
	use Monet.Query.Where

	defmacro __using__(_) do
		quote do
			use Monet.Query.Where
			alias Monet.Query.Select
		end
	end

	alias __MODULE__

	@enforce_keys [:select, :from, :where, :order, :limit, :offset]
	defstruct @enforce_keys

	def new() do
		%Select{
			from: [],
			order: nil,
			limit: nil,
			offset: nil,
			select: nil,
			where: Where.new(),
		}
	end

	@doc """
	Columns to select. If not called, will select *.
	Can can called multiple times. Can be called with an array, or a string.
	This can really be anything and it's best to think of it as the test that
	is placed between the `select` and the `from.
	"""
	def columns(q, [first | columns]) do
		columns = Enum.reduce(columns, [first], fn c, acc -> [acc, ", ", c] end)
		append_columns(q, columns)
	end
	def columns(s, column), do: append_columns(s, column)
	defp append_columns(%{select: nil} = s, columns), do: %Select{s | select: columns}
	defp append_columns(s, columns), do: %Select{s | select: [s.select, ", ", columns]}

	@doc """
	Table to select from. Can be called multiple times. This essentially becomes
	what gets placed between the "from" and the "where".

	You could do:
			Select.from(s, "users")
			# OR
			Select.from(s, "(select 1 from another) x")
	"""
	def from(s, from), do: %Select{s | from: [s.from, from]}

	@doc """
	Join tables. There's no magic here. Doesn't know anything
	about your tables (aka, you need to tell it what to join on):

			Select.join(s, "table b on a.id = b.id")

	This is just a shorthand for `from/2` but it injects the word
	" [left|right|full]? join " for you
	"""
	def join(s, table), do: %Select{s | from: [s.from, [" join ", table]]}
	def join(s, :left, table), do: %Select{s | from: [s.from, [" left join ", table]]}
	def join(s, :right, table), do: %Select{s | from: [s.from, [" right join ", table]]}
	def join(s, :full, table), do: %Select{s | from: [s.from, [" full join ", table]]}

	def order(s, order), do: append_order(s, order)
	def order(s, order, true), do: append_order(s, order)
	def order(s, order, false), do: append_order(s, [order, " desc"])
	defp append_order(%{order: nil} = s, order), do: %Select{s | order: [order]}
	defp append_order(%{order: existing} = s, order), do: %Select{s | order: [existing, ", ", order]}

	def limit(s, limit) when is_integer(limit), do: %Select{s | limit: limit}
	def offset(s, offset) when is_integer(offset), do: %Select{s | offset: offset}

	def exec(s, pool \\ Monet) do
		{sql, args} = to_sql(s)
		Monet.query(pool, sql, args)
	end

	def exec!(s, pool \\ Monet) do
		case exec(s, pool) do
			{:ok, result} -> result
			{:error, err} -> raise err
		end
	end

	def to_sql(s) do
		{where, args} = Where.to_sql(s.where)
		sql = ["select ", s.select || "*", " from ", s.from, where]

		sql = case s.order do
			nil -> sql
			order -> [sql, " order by ", order]
		end

		sql = case s.limit do
			nil -> sql
			limit -> [sql, " limit ", Integer.to_string(limit)]
		end

		sql = case s.offset do
			nil -> sql
			offset -> [sql, " offset ", Integer.to_string(offset)]
		end

		{sql, args}
	end

end

defimpl Inspect, for: Monet.Query.Select do
	def inspect(q, opts) do
		import Inspect.Algebra
		{sql, values} = Monet.Query.Select.to_sql(q)
		sql = :erlang.iolist_to_binary(sql)
		sql = Regex.split(~r/ (from|join|where|order by|limit|offset) /, sql, include_captures: true)
		docs = fold_doc(sql, fn
			<<" ", doc::binary>>, acc when doc in ["from ", "join", "where ", "order by ", "limit ", "offset "] -> concat([break("\n"), doc, acc])
			doc, acc -> concat(doc, acc)
		end)
		concat [docs, break("\n"), to_doc(values, opts)]
	end
end
