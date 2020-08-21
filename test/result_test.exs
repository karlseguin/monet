defmodule Monet.Tests.Result do
	use Monet.Tests.Base

	setup_all do
		connect()

		Monet.query!("drop table if exists result_test")
		Monet.query!("create table result_test (id int, name text)")

		Monet.query!("
			insert into result_test
			values (?, ?), (?, ?), (?, ?)
		", [1, "Leto", 2, "Jessica", 3, "Paul"])

		:ok
	end

	test "enumerates a list of lists" do
		result = "select * from result_test order by id"
		|> Monet.query!()
		|> Enum.map(fn [id, name] -> [name, id] end)

		assert result == [
			["Leto", 1], ["Jessica", 2], ["Paul", 3],
		]
	end

	test "jason encodes list of lists" do
		result = "select * from result_test order by id"
		|> Monet.query!()
		|> Jason.encode!()
		|> Jason.decode!()

		assert result == [
			[1, "Leto"], [2, "Jessica"], [3, "Paul"],
		]
	end

	test "enumerates a list of maps" do
		result = "select * from result_test order by id"
		|> Monet.query!()
		|> Monet.as_map()
		|> Enum.to_list()

		assert result == [
			%{"id" => 1, "name" => "Leto"},
			%{"id" => 2, "name" => "Jessica"},
			%{"id" => 3, "name" => "Paul"}
		]
	end

	test "json encodes a list of maps" do
		result = "select * from result_test order by id"
		|> Monet.query!()
		|> Monet.as_map()
		|> Jason.encode!()
		|> Jason.decode!()

		assert result == [
			%{"id" => 1, "name" => "Leto"},
			%{"id" => 2, "name" => "Jessica"},
			%{"id" => 3, "name" => "Paul"}
		]
	end

	test "enumerates a list of maps with atom columns" do
		result = "select * from result_test order by id"
		|> Monet.query!()
		|> Monet.as_map(columns: :atoms)
		|> Enum.to_list()

		assert result == [
			%{id: 1, name: "Leto"},
			%{id: 2, name: "Jessica"},
			%{id: 3, name: "Paul"}
		]
	end

	test "rows helper" do
		result = Monet.query!("select 1, 2")
		assert Monet.rows(result) == {:ok, [[1, 2]]}
		assert Monet.rows({:ok, result}) == {:ok, [[1, 2]]}
		assert Monet.rows!(result) == [[1, 2]]
		assert Monet.rows!({:ok, result}) == [[1, 2]]

		result = Monet.query!("select 1, 2 union all select 3, 4")
		assert Monet.rows(result) == {:ok, [[1, 2], [3, 4]]}
		assert Monet.rows({:ok, result}) == {:ok, [[1, 2], [3, 4]]}
		assert Monet.rows!(result) == [[1, 2], [3, 4]]
		assert Monet.rows!({:ok, result}) == [[1, 2], [3, 4]]

		assert Monet.rows(Monet.query!("select 1 where false")) == {:ok, []}
	end

	test "row helper" do
		result = Monet.query!("select 1, 2")
		assert Monet.row(result) == {:ok, [1, 2]}
		assert Monet.row({:ok, result}) == {:ok, [1, 2]}
		assert Monet.row!(result) == [1, 2]
		assert Monet.row!({:ok, result}) == [1, 2]

		result = Monet.query!("select 1 where false")
		assert Monet.row(result) == {:ok, nil}
		assert Monet.row!({:ok, result}) == nil

		result = Monet.query!("select 1, 2 union all select 3, 4")
		assert Monet.row(result) == {:error, %Monet.Error{code: nil, details: nil, message: "row called but multiple rows returned", source: :client}}
		assert Monet.row({:ok, result}) == {:error, %Monet.Error{code: nil, details: nil, message: "row called but multiple rows returned", source: :client}}

		assert_raise Monet.Error, "client row called but multiple rows returned", fn -> Monet.row!(result) end
	end

	test "map helper" do
		result = Monet.query!("select 1 a, 2 b")
		assert Monet.map(result) == {:ok, %{"a" => 1, "b" => 2}}
		assert Monet.map({:ok, result}, columns: :atoms) == {:ok, %{a: 1, b: 2}}
		assert Monet.map!(result, columns: :atoms) == %{a: 1, b: 2}
		assert Monet.map!({:ok, result}) == %{"a" => 1, "b" => 2}

		result = Monet.query!("select 1 where false")
		assert Monet.map(result) == {:ok, nil}
		assert Monet.map!({:ok, result}) == nil

		result = Monet.query!("select 1, 2 union all select 3, 4")
		assert Monet.map(result) == {:error, %Monet.Error{code: nil, details: nil, message: "map called but multiple rows returned", source: :client}}
		assert Monet.map({:ok, result}) == {:error, %Monet.Error{code: nil, details: nil, message: "map called but multiple rows returned", source: :client}}

		assert_raise Monet.Error, "client map called but multiple rows returned", fn -> Monet.map!(result) end
	end

	test "maps helper" do
		result = Monet.query!("select 1 a, 2 b union all select 4, 5")
		assert Monet.maps(result) == {:ok, [%{"a" => 1, "b" => 2}, %{"a" => 4, "b" => 5}]}
		assert Monet.maps({:ok, result}, columns: :atoms) == {:ok, [%{a: 1, b: 2}, %{a: 4, b: 5}]}
		assert Monet.maps!(result, columns: :atoms) == [%{a: 1, b: 2}, %{a: 4, b: 5}]
		assert Monet.maps!({:ok, result}) == [%{"a" => 1, "b" => 2}, %{"a" => 4, "b" => 5}]

		result = Monet.query!("select 1 where false")
		assert Monet.maps(result) == {:ok, []}
		assert Monet.maps!({:ok, result}) == []
	end

	test "scalar helper" do
		result = Monet.query!("select 9001")
		assert Monet.scalar(result) == {:ok, 9001}
		assert Monet.scalar({:ok, result}) == {:ok, 9001}
		assert Monet.scalar!(result) == 9001
		assert Monet.scalar!({:ok, result}) == 9001

		result = Monet.query!("select 1 union all select 3")
		assert Monet.scalar(result) == {:error, %Monet.Error{code: nil, details: nil, message: "scalar called but multiple rows returned", source: :client}}
		assert Monet.scalar({:ok, result}) == {:error, %Monet.Error{code: nil, details: nil, message: "scalar called but multiple rows returned", source: :client}}
		assert_raise Monet.Error, "client scalar called but multiple rows returned", fn -> Monet.scalar!(result) end

		result = Monet.query!("select 1, 2")
		assert Monet.scalar(result) == {:error, %Monet.Error{code: nil, details: nil, message: "scalar called but multiple columns returned", source: :client}}
		assert Monet.scalar({:ok, result}) == {:error, %Monet.Error{code: nil, details: nil, message: "scalar called but multiple columns returned", source: :client}}
		assert_raise Monet.Error, "client scalar called but multiple columns returned", fn -> Monet.scalar!(result) end
	end
end
