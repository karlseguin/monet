defmodule Monet.Query.Cursor do
	@moduledoc """
	Fetches paged results using cursors and generates new next/prev cursors
	from the result. This requires a unique integer cursor column.

	There are subtle differences between paging forward and backwards and this
	interacts with whether we're sorting ascendingly and descendinly. Also, because
	the sorting column may not be unique, the filter ends up looking. Say we want
	to sort by `price`, we need to do:

			where (price > $1 or (price = $1 and cursor > $2)) order by price, cursor

	If you ask for `perpage: 25` it'll fetch 26 records to see if there are more
	results. If so, a `next` link is generated based on the price+cursor of the 25th
	record.

	Now consider the case where we want to fetch the previous page while ordering
	by ascending price. This is trickier, because if we just did:

			where (price < $1 or (price = $1 and cursor < $2)) order by price, cursor

	We'd get the wrong results. Consider this data with a perpage of 2:

			id, price, cursor
			1,  1.00,  1
			2,  2.00,  2
			3,  3.00,  3
			4,  4.00,  4
			5,  5.00,  5

	Moving forward, we'd get the following ids:

			[1, 2] -> next -> [3, 4] -> next [5]

	Now, following the `prev` link from this last page with the above query, we'd
	end up with [1, 2].  Instead, what we need to do is reverse the order:

			where (price < $1 or (price = $1 and cursor < $2)) order by price DESC, cursor DESC

	Now we'll get the right page, but in the wrong order [4, 3]. So we need to reverse
	it once more.

	All of this ordering and filtering isn't too complicated: there are only 4
	combinations of prev/next and asc/desc. You also need to "overfetch" (get +1
	records to know if there are "more"), and remove it if present. Again, none
	of it is complicated, but it takes some attetion to do it efficient - moreso
	with [linked]-lists.

	There are 2 parts to this cursor. The first is responsible for doing
	all of the above. The second (much smaller) part deals with iterating through
	the results and building the next/prev cursors. This is done in the name of
	efficiency. In most cases, we need the last row (the last item in our list).
	In Elixir, this is an O(N) operation. However, since your code likely needs
	to iterate the results anyways (to build the payload), we can combine the two
	together. As such, the cursor acts as a sort of generator.

	This iteration/generator phase can be ignored. The cursor that's returned as
	part of Select.cursor is fully materialized and contains all of the necessary
	data.
	"""

	alias __MODULE__
	alias Monet.Query.Select

	@enforce_key [
		:more,
		:rows,
		:path,
		:last,
		:first,
	]

	defstruct @enforce_key

	def new(select, opts) do
		cursor = opts[:cursor]
		conn = Keyword.fetch!(opts, :conn)
		sort = Keyword.fetch!(opts, :sort)
		asc = Keyword.get(opts, :asc, true)
		pool = Keyword.get(opts, :pool, Monet)
		perpage = Keyword.get(opts, :perpage, 20)
		column = Keyword.get(opts, :column, "cursor")

		{:ok, dir, select} = build_where(select, sort, column, asc, cursor)
		prev? = dir == :prev

		asc = case prev? do
			true -> !asc
			false -> asc
		end

		select = select
		|> build_select(sort, column)
		|> build_order(asc, sort, column)
		|> Select.limit(perpage + 1)

		%{row_count: count, rows: rows} = Select.exec!(select, pool)
		more = count > perpage

		# If we're moving to the previous page, our results is reversed.
		# We're going to re-reverse it. If we have an overfetch, we'll pop it off
		# now (that it's cheap).
		{rows, more} = cond do
			prev? && more ->
				[_overfetch | rows] = Enum.reverse(rows)
				{rows, :prev}
			prev? -> {Enum.reverse(rows), false}
			true -> {rows, more}
		end

		first = case rows do
			[[cursor | _] | _] -> cursor # pop off the first column of the first row
			_ -> nil
		end

		path = (conn.query_string || "")
		|> String.splitter("&", trim: true)
		|> Enum.reduce([conn.request_path, "?"], fn
			<<"cursor=", _::binary>>, acc -> acc
			param, acc -> [acc, param, "&"]
		end)
		|> :erlang.iolist_to_binary()

		%Cursor{more: more, rows: rows, first: first, path: path}
	end

	# prev/asc
	defp build_where(select, sort, cursor_column, true, <<"p", cursor::binary>>) do
		build_where_sql(select, sort, cursor_column, " < ", cursor, :prev)
	end

	# prev/desc
	defp build_where(select, sort, cursor_column, false, <<"p", cursor::binary>>) do
		build_where_sql(select, sort, cursor_column, " > ", cursor, :prev)
	end

	# next/asc
	defp build_where(select, sort, cursor_column, true, <<"n", cursor::binary>>) do
		build_where_sql(select,	sort, cursor_column, " > ", cursor, :next)
	end

	# next/desc
	defp build_where(select, sort, cursor_column, false, <<"n", cursor::binary>>) do
		build_where_sql(select, sort, cursor_column, " < ", cursor, :next)
	end

	defp build_where(select, _sort, _cursor_column, _asc, _cursor) do
		{:ok, :none, select}
	end

	defp build_where_sql(select, {sort, type}, cursor_column, op, cursor, dir) do
		with [cursor_value, sort_value] <- :binary.split(cursor, "_"),
		     {cursor_value, ""} <- Integer.parse(cursor_value),
		     {:ok, sort_value} <- decode_sort_value(sort_value, type)
		do
			select = select
			|> Select.param(sort_value)
			|> Select.param(sort_value)
			|> Select.param(cursor_value)

			sql = ["(",
				sort, op, ??, " or (",
				sort, " = ", ??, " and ", cursor_column, op, ??, ")",
			")"]

			{:ok, dir, Select.where(select, :sql, sql)}
		else
			_ -> {:ok, :none, select}
		end
	end

	defp build_select(select, {sort, type}, cursor_column) do
		column = [cursor_column, " || '_' || ", encode_sort_column(sort, type)]
		prepend_select(select, column)
	end

	defp prepend_select(%{select: nil} = select, column) do
		%{select | select: column}
	end

	defp prepend_select(%{select: columns} = select, column) do
		%{select | select: [column, ", ", columns]}
	end

	defp build_order(select, asc, {sort, _type}, cursor_column) do
		select = Select.order(select, sort, asc)
		case cursor_column == nil do
			true -> select
			false -> Select.order(select, cursor_column, asc)
		end
	end

	defp decode_sort_value(n, :int) do
		case Integer.parse(n) do
			{n, ""} -> {:ok, n}
			_ -> :error
		end
	end

	defp decode_sort_value(n, :float) do
		case Float.parse(n) do
			{n, ""} -> {:ok, n}
			_ -> :error
		end
	end

	defp decode_sort_value(n, :timestamp) do
		case Integer.parse(n) do
			{n, ""} -> DateTime.from_unix(n, :microsecond)
			_ -> :error
		end
	end

	# doesn't seem to be a better way to extract the time as microseconds?!
	defp encode_sort_column(n, :timestamp) do
		["(sys.epoch(", n, ") * 1000000 + cast(((\"second\"(", n, ") - floor(\"second\"(", n, ")))) * 1000000 as int))"]
	end

	defp encode_sort_column(n, _), do: n

	# The following functions deal with iterate over the cursor, as exposed
	# in Select.reduce/3
	def next(%{first: nil} = cursor) do
		{:empty, cursor}
	end

	# If we've over-fetched, then the 2nd last row is really the last one
	def next(%{more: true, rows: [[last | row], _overfetched]} = cursor) do
		{:last, row, %Cursor{cursor | rows: [], last: last}}
	end

	# If we haven't overfetched, then the last row is the last row
	def next(%{more: false, rows: [[last | row]]} = cursor) do
		{:last, row, %Cursor{cursor | rows: [], last: last}}
	end

	# Special case the more: prev flag we sent for the case where we did overfetch
	# (there IS more) but we dropped the extra row because we had to reverse the order
	# (Either we do this, or we APPEND the extra row just to signal that there is
	# more data)
	def next(%{more: :prev, rows: [[last | row]]} = cursor) do
		{:last, row, %Cursor{cursor | rows: [], last: last}}
	end

	# just a normal row
	def next(%{rows: [[_cursor | row] | rows]} = cursor) do
		{:row, row, %Cursor{cursor | rows: rows}}
	end

	def next(%{rows: [], more: more, path: path} = cursor) do
		more = case more do
			:prev -> true
			more -> more
		end

		prev = [path, "cursor=p", cursor.first]
		next = [path, "cursor=n", cursor.last]
		{:paging, more, prev, next}
	end
end
