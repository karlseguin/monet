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
end
