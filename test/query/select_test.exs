defmodule Monet.Tests.Query.Select do
	use Monet.Tests.Base

	use Monet.Query.Select

	test "basic select" do
		{sql, []} = Select.new()
		|> Select.from("table")
		|> render()

		assert sql == "select * from table"
	end

	test "select with columns" do
		{sql, []} = Select.new()
		|> Select.columns("id")
		|> Select.from("table")
		|> render()

		assert sql == "select id from table"
	end

	test "select with columns (2)" do
		{sql, []} = Select.new()
		|> Select.columns("id")
		|> Select.columns("name")
		|> Select.columns(~w(created updated))
		|> Select.from("table")
		|> render()

		assert sql == "select id, name, created, updated from table"
	end

	test "joins" do
		{sql, []} = Select.new()
		|> Select.from("ta a")
		|> Select.join("tb b on a.id = b.a_id")
		|> Select.join(:left, "tc c on a.id = c.a_id")
		|> Select.join(:right, "td d on a.id = d.a_id")
		|> Select.join(:full, "te e on a.id = e.a_id")
		|> render()

		assert sql == "select * from ta a join tb b on a.id = b.a_id left join tc c on a.id = c.a_id right join td d on a.id = d.a_id full join te e on a.id = e.a_id"
	end

	test "order" do
		base = Select.from(Select.new(), "t")

		{sql, []} = base |> Select.order("x") |> render()
		assert sql == "select * from t order by x"

		{sql, []} = base |> Select.order("x", true) |> render()
		assert sql == "select * from t order by x"

		{sql, []} = base |> Select.order("x", false) |> render()
		assert sql == "select * from t order by x desc"

		{sql, []} = base |> Select.order("x", false) |> Select.order("y") |> render()
		assert sql == "select * from t order by x desc, y"
	end

	test "limit / offset" do
		base = Select.from(Select.new(), "t")

		{sql, []} = base |> Select.limit(10) |> render()
		assert sql == "select * from t limit 10"

		{sql, []} = base |> Select.offset(20) |> render()
		assert sql == "select * from t offset 20"

		{sql, []} = base |> Select.limit(20) |> Select.offset(40) |> render()
		assert sql == "select * from t limit 20 offset 40"
	end

	test "order+group+limit+offset" do
		{sql, []} = Select.new()
		|> Select.from("t")
		|> Select.order("x")
		|> Select.order("y", false)
		|> Select.group("name")
		|> Select.offset(1000)
		|> Select.limit(100)
		|> render()

		assert sql == "select * from t group by name order by x, y desc limit 100 offset 1000"
	end

	test "simple filters" do
		{sql, args} = Select.new()
		|> Select.from("t")
		|> Select.where("a1", :eq, 1)
		|> Select.where("a2", :eq, :atom)
		|> Select.where("a3", :eq, true)
		|> Select.where("a4", :eq, "over")
		|> Select.where("a5", :eq, nil)
		|> Select.where("b1", :ne, 9000)
		|> Select.where("b2", :ne, :atom)
		|> Select.where("b3", :ne, false)
		|> Select.where("b4", :ne, "dune")
		|> Select.where("b5", :ne, nil)
		|> Select.where("c1", :gt, 10)
		|> Select.where("c2", :gte, 11)
		|> Select.where("d1", :lt, 12)
		|> Select.where("d2", :lte, 13)
		|> Select.where("e1", :like, "abc")
		|> Select.where("f1", :any, [])
		|> Select.where("f2", :any, 100)
		|> Select.where("f3", :any, [101])
		|> Select.where("f4", :any, [102, 103])
		|> render()

		assert sql == flatten("select * from t
			where a1 = ? and a2 = ? and a3 = ? and a4 = ? and a5 is null
			and b1 <> ? and b2 <> ? and b3 <> ? and b4 <> ? and b5 is not null
			and c1 > ? and c2 >= ? and d1 < ? and d2 <= ?
			and e1 like ?
			and f2 = ? and (f3 = ?) and (f4 = ? or f4 = ?)")

		assert args == [
			1, "atom", true, "over",
			9000, "atom", false, "dune",
			10, 11, 12, 13, "abc",
			100, 101, 102, 103
		]
	end

	test "null filters" do
		{sql, args} = Select.new()
		|> Select.from("t")
		|> Select.where_ignore_nil("a1", :eq, 1)
		|> Select.where_ignore_nil("a2", :eq, :atom)
		|> Select.where_ignore_nil("a3", :eq, true)
		|> Select.where_ignore_nil("a4", :eq, "over")
		|> Select.where_ignore_nil("a5", :eq, nil)
		|> Select.where_ignore_nil("b1", :ne, 9000)
		|> Select.where_ignore_nil("b2", :ne, :atom)
		|> Select.where_ignore_nil("b3", :ne, false)
		|> Select.where_ignore_nil("b4", :ne, "dune")
		|> Select.where_ignore_nil("b5", :ne, nil)
		|> Select.where_ignore_nil("c1", :gt, 10)
		|> Select.where_ignore_nil("c2", :gte, 11)
		|> Select.where_ignore_nil("d1", :lt, 12)
		|> Select.where_ignore_nil("d2", :lte, 13)
		|> render()

		assert sql == flatten("select * from t
			where a1 = ? and a2 = ? and a3 = ? and a4 = ?
			and b1 <> ? and b2 <> ? and b3 <> ? and b4 <> ?
			and c1 > ? and c2 >= ? and d1 < ? and d2 <= ?")

		assert args == [
			1, "atom", true, "over",
			9000, "atom", false, "dune",
			10, 11, 12, 13
		]
	end

	test "where group" do
		{sql, args} = Select.new()
		|> Select.from("t")
		|> Select.where_or(fn w -> w |> eq("name", "goku") |> gt("power", 9000) end)
		|> Select.where("a", :eq, true)
		|> render()

		assert sql == "select * from t where (name = ? or power > ?)  and a = ?"
		assert args == ["goku", 9000, true]
	end

	test "any" do
		{sql, args} = Select.new()
		|> Select.from("t")
		|> Select.where("a1", :any, 1)
		|> render()
		assert sql == flatten("select * from t where a1 = ?")
		assert args == [1]

		{sql, args} = Select.new()
		|> Select.from("t")
		|> Select.where("a1", :any, [1, 2])
		|> render()
		assert sql == flatten("select * from t where (a1 = ? or a1 = ?)")
		assert args == [1, 2]

		{sql, args} = Select.new()
		|> Select.from("t")
		|> Select.where("x1", :eq, true)
		|> Select.where("a1", :any, [1, 2])
		|> Select.where("z1", :eq, false)
		|> render()

		assert sql == flatten("select * from t where x1 = ? and (a1 = ? or a1 = ?) and z1 = ?")
		assert args == [true, 1, 2, false]
	end

	test "exec" do
		connect()
		assert Select.new()
		|> Select.columns("1")
		|> Select.from("sys.tables")
		|> Select.where("1", :eq, 1)
		|> Select.limit(1)
		|> Select.exec!()
		|> Monet.scalar!() == 1
	end

	test "exec with syntax error" do
		connect()
		select = Select.new()
		|> Select.columns("1")
		|> Select.from("sys.table t")
		|> Select.where("1", :eq, 1)
		|> Select.where("1", :eq, 1)
		|> Select.where("1", :eq, 1)
		|> Select.where("1", :eq, 1)
		|> Select.where("1", :eq, 1)
		|> Select.where(")", :eq, 1)
		|> Select.limit(1)

		try do
			Select.exec!(select)
			flunk("expecting failure")
		rescue
			e in Monet.Error ->
				assert e.details == "select 1\nfrom sys.table t\nwhere 1 = ? and 1 = ? and 1 = ? and 1 = ? and 1 = ? and ) = ?\nlimit 1\n[1, 1, 1, 1, 1, 1]"
		end
	end

	test "inspect" do
		import ExUnit.CaptureIO

		select = Select.new()
		|> Select.columns("c")
		|> Select.from("t")
		|> Select.where("w", :eq, 1)
		|> Select.order("x")
		|> Select.order("y", false)
		|> Select.group("name")
		|> Select.offset(1000)
		|> Select.limit(100)

		assert capture_io(fn ->
			IO.inspect(select)
		end) == "select c\nfrom t\nwhere w = ? group by name\norder by x, y desc\nlimit 100\noffset 1000\n[1]\n"
	end

	defp render(select) do
		{sql, args} = Select.to_sql(select)

		sql = sql
		|> :erlang.iolist_to_binary()
		|> String.trim()
		{sql, args}
	end

	defp flatten(sql), do: String.replace(sql, ~r/\s+/, " ")
end
