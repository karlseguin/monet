defmodule Monet.Tests.Query.Cursor do
	use Monet.Tests.Base

	use Monet.Query.Select

	setup_all do
		connect()
		Monet.query!("drop table if exists cursor_test")
		Monet.query!("
			create table cursor_test (
				id int,
				cursor int,
				int_sort int,
				float_sort float,
				timestamptz_sort timestamptz
			)
		")

		# rows 3 and 4 hae the same sort values to test that equal sorts behave correctly
		Monet.query!("
			insert into cursor_test (id, cursor, int_sort, float_sort, timestamptz_sort) values
				(1, 1, 1, 1.1, now() + 1),
				(2, 2, 2, 2.2, now() + 2),
				(3, 3, 3, 3.3, now() + 3),
				(4, 4, 3, 3.3, now() + 3),
				(5, 5, 5, 5.5, now() + 5)
		",[])
		:ok
	end

	test "next/prev" do
		assert Select.new()
		|> Select.columns("id")
		|> Select.from("cursor_test")
		|> Select.cursor(perpage: 2, sort: {"int_sort", :int}, conn: %{request_path: "/data", query_string: "x=1&perpage=2"})
		|> Select.reduce(nil, fn
			{:paging, _more, prev, next}, nil -> {:erlang.iolist_to_binary(prev), :erlang.iolist_to_binary(next)}
			_, _ -> nil
		end) == {"/data?x=1&perpage=2&cursor=p1_1", "/data?x=1&perpage=2&cursor=n2_2"}

		assert Select.new()
		|> Select.columns("id")
		|> Select.from("cursor_test")
		|> Select.cursor(perpage: 2, cursor: "n2_2", sort: {"int_sort", :int}, conn: %{request_path: "/data", query_string: "x=1&perpage=2&cursor=n4_3"})
		|> Select.reduce(nil, fn
			{:paging, _more, prev, next}, nil -> {:erlang.iolist_to_binary(prev), :erlang.iolist_to_binary(next)}
			_, _ -> nil
		end) == {"/data?x=1&perpage=2&cursor=p3_3", "/data?x=1&perpage=2&cursor=n4_3"}
	end

	test "int asc" do
		assert_cursor [[1, 2], [3, 4], [5]], fn ->
			Select.new()
			|> Select.columns("id")
			|> Select.from("cursor_test")
		end, sort: {"int_sort", :int}, cursor: "invalid"
	end

	test "int desc" do
		assert_cursor [[5, 4], [3, 2], [1]], fn ->
			Select.new()
			|> Select.columns("id")
			|> Select.from("cursor_test")
		end, sort: {"int_sort", :int}, asc: false, cursor: "n_nope"
	end

	test "timestamp asc" do
		assert_cursor [[1, 2], [3, 4], [5]], fn ->
			Select.new()
			|> Select.columns("id")
			|> Select.from("cursor_test")
		end, sort: {"timestamptz_sort", :timestamp}
	end

	test "timestamp desc" do
		assert_cursor [[5, 4], [3, 2], [1]], fn ->
			Select.new()
			|> Select.columns("id")
			|> Select.from("cursor_test")
		end, sort: {"timestamptz_sort", :timestamp}, asc: false
	end

	test "float asc" do
		assert_cursor [[1, 2], [3, 4], [5]], fn ->
			Select.new()
			|> Select.columns("id")
			|> Select.from("cursor_test")
		end, sort: {"float_sort", :float}
	end

	test "float desc" do
		assert_cursor [[5, 4], [3, 2], [1]], fn ->
			Select.new()
			|> Select.columns("id")
			|> Select.from("cursor_test")
		end, sort: {"float_sort", :float}, asc: false
	end

	defp assert_cursor(expected, select, opts) do
		opts = Keyword.merge([
			perpage: 2,
			asc: true,
			column: "cursor",
			conn: %{request_path: "", query_string: ""}
		], opts)

		{_opts, last} = Enum.reduce(expected, {opts, nil}, fn expected, {opts, _} ->
			page = select.()
			|> Select.cursor(opts)
			|> reduce()

			assert page.rows == expected
			{Keyword.put(opts, :cursor, page.next), page}
		end)

		# not go in reverse, star
		expected = expected |> Enum.reverse() |> tl()
		opts = Keyword.put(opts, :cursor, last.prev)
		Enum.reduce(expected, opts, fn expected, opts ->
			page = select.()
			|> Select.cursor(opts)
			|> reduce()

			assert page.rows == expected
			Keyword.put(opts, :cursor, page.prev)
		end)
	end

	defp reduce(cursor) do
		Select.reduce(cursor, {[], nil}, fn
			{:row, [id]}, {rows, paging} -> {[id | rows], paging}
			:empty, {[], nil} -> %{rows: [], more: false}
			{:paging, more, prev, next}, {rows, nil} ->

				prev = prev |> :erlang.iolist_to_binary() |> URI.parse() |> Map.get(:query) |> URI.decode_query() |> Map.get("cursor")
				next = next |> :erlang.iolist_to_binary() |> URI.parse() |> Map.get(:query) |> URI.decode_query() |> Map.get("cursor")

				%{
					more: more,
					next: next,
					prev: prev,
					rows: Enum.reverse(rows),
				}
		end)
	end
end
