defmodule Monet.Query.Where do
	alias __MODULE__

	defmacro __using__(_) do
		quote location: :keep do
			alias Monet.Query.Where

			def where_ignore_nil(q, _, _, nil), do: q
			def where_ignore_nil(q, column, op, value), do: where(q, column, op, value)

			def where(q, column, :eq, value) do
				%{q | where: Where.eq(q.where, column, value)}
			end

			def where(q, column, :ne, value) do
				%{q | where: Where.ne(q.where, column, value)}
			end

			def where(q, column, :gt, value) do
				%{q | where: Where.gt(q.where, column, value)}
			end

			def where(q, column, :gte, value) do
				%{q | where: Where.gte(q.where, column, value)}
			end

			def where(q, column, :lt, value) do
				%{q | where: Where.lt(q.where, column, value)}
			end

			def where(q, column, :lte, value) do
				%{q | where: Where.lte(q.where, column, value)}
			end

			def where(q, column, :like, value) do
				%{q | where: Where.like(q.where, column, value)}
			end

			@where_op [:eq, :ne, :gt, :gte, :lt, :lte, :like]
			defmacro where_and(q, fun) do
				fun = Macro.postwalk(fun, fn
					{op, line, args} when op in @where_op -> {{:., line, [{:__aliases__, line, [:Monet, :Query, :Where]}, op]}, line, args}
					expr -> expr
				end)
				quote location: :keep do
					where = unquote(q).where
					where = Where.where_fun(where, unquote(fun), " and ")
					%{unquote(q) | where: where}
				end
			end

			defmacro where_or(q, fun) do
				fun = Macro.postwalk(fun, fn
					{op, line, args} when op in @where_op -> {{:., line, [{:__aliases__, line, [:Monet, :Query, :Where]}, op]}, line, args}
					expr -> expr
				end)
				quote location: :keep do
					where = unquote(q).where
					where = Where.where_fun(where, unquote(fun), " or ")
					%{unquote(q) | where: where}
				end
			end
		end
	end

	@enforce_keys [:sql, :op, :values]
	defstruct @enforce_keys

	def new() do
		%Where{
			sql: nil,
			op: " and ",
			values: [],
		}
	end

	# need this so that the is_atom guard doesn't pick it up and try to convert
	# our true/false atom to a string
	def eq(w, column, nil), do: append(w, [column, " is null"])
	def eq(w, column, value) when is_boolean(value), do: append(w, column, " = ", value)
	def eq(w, column, value) when is_atom(value), do: eq(w, column, Atom.to_string(value))
	def eq(w, column, value), do: append(w, column, " = ", value)

	def ne(w, column, nil), do: append(w, [column, " is not null"])
	def ne(w, column, value) when is_boolean(value), do: append(w, column, " <> ", value)
	def ne(w, column, value) when is_atom(value), do: ne(w, column, Atom.to_string(value))
	def ne(w, column, value), do: append(w, column, " <> ", value)

	def gt(w, column, value), do: append(w, column, " > ", value)
	def gte(w, column, value), do: append(w, column, " >= ", value)

	def lt(w, column, value), do: append(w, column, " < ", value)
	def lte(w, column, value), do: append(w, column, " <= ", value)

	def like(w, column, value), do: append(w, column, " like ", value)

	def where_fun(where, fun, op) do
		outer = case where.sql do
			nil -> [" where ("]
			sql -> [sql, where.op, "("]
		end
		where = %Where{where | sql: :group, op: op}
		where = fun.(where)
		%Where{where | sql: [outer, where.sql, ") "], op: " and "}
	end

	def to_sql(w), do: {w.sql || "", Enum.reverse(w.values)}

	defp append(w, filter) do
		sql = case w.sql do
			nil -> [" where ", filter]
			:group -> [filter]
			acc -> [acc, w.op, filter]
		end
		%Where{w | sql: sql}
	end

	defp append(w, column, op, value) do
		w = append(w, [column, op, ??])
		%Where{w | values: [value | w.values]}
	end
end
