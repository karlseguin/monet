defmodule Monet.Tests.Monet do
	use Monet.Tests.Base

	alias Monet.Tests.Generator

	@fuzz_count 100

	@sql_types [
		char1_col: [sql: "char(1)", type: :utf8, args: 1],
		char5_col: [sql: "char(5)", type: :utf8, args: 5],
		char100_col: [sql: "char(100)", type: :utf8, args: 100],
		varchar_col: [sql: "varchar(100)", type: :utf8, args: {0, 100}],
		text_col: [sql: "text", type: :utf8, args: {0, 1000}],
		bool_col: [sql: "bool", type: :bool],
		tinyint_col: [sql: "tinyint", type: :int, args: 8],
		smallint_col: [sql: "smallint", type: :int, args: 16],
		int_col: [sql: "int", type: :int, args: 32],
		bigint_col: [sql: "bigint", type: :int, args: 64],
		hugeint_col: [sql: "hugeint", type: :int, args: 128],
		double_col: [sql: "double", type: :float, args: 64],
		decimal_col: [sql: "decimal", type: :decimal],
		date_col: [sql: "date", type: :date],
		time0_col: [sql: "time", type: :time],
		time3_col: [sql: "time(3)", type: :time, args: 3],
		time6_col: [sql: "time(6)", type: :time, args: 6],
		blob_col: [sql: "blob", type: :blob],
		timestamp0_col: [sql: "timestamp(0)", type: :naivedatetime],
		timestamp3_col: [sql: "timestamp(3)", type: :naivedatetime, args: 3],
		timestamp6_col: [sql: "timestamp(6)", type: :naivedatetime, args: 6],
		timestamptz0_col: [sql: "timestamp(0) with time zone", type: :datetime],
		timestamptz3_col: [sql: "timestamp(3) with time zone", type: :datetime, args: 3],
		timestamptz6_col: [sql: "timestamp(6) with time zone", type: :datetime, args: 6],
		json_col: [sql: "json", type: :json],
		uuid_col: [sql: "uuid", type: :uuid]
	]

	setup_all do
		connect()
		:ok
	end

	test "deallocates prepared" do
		Monet.query!("select 1 - ?, 'a'", [1])
		assert Monet.query!("select 1 from sys.prepared_statements").row_count == 0

		# even on error
		assert {:error, _} = Monet.query("select 1 - ?", ["a'"])
		assert Monet.query!("select * from sys.prepared_statements").row_count == 0
	end

	test "result with no rows" do
		result = Monet.query!("select 1 where false")
		assert result.rows == []
		assert result.row_count == 0
		assert Enum.count(result) == 0
	end

	test "json escapes" do
		Monet.query!("drop table if exists sql_types")
		Monet.query!("create table sql_types (id serial, j json)")

		t = fn m->
			json = Jason.encode!(m)
			Monet.query!("insert into sql_types (j) values (?)", [json])
			[actual] = Monet.query!("select j from sql_types order by id desc limit 1").rows
			assert Jason.decode!(actual, keys: :atoms) == m
		end

		t.(%{name: "\\"})
		t.(%{name: "\'"})
		t.(%{name: "\'\t\\\""})
	end

	test "fuzz insert and select sql types" do
		create = @sql_types
		|> Enum.map(fn {name, opts} -> "#{name} #{opts[:sql]} null" end)
		|> Enum.join(", ")

		columns = @sql_types
		|> Enum.map(fn {name, _} -> name end)
		|> Enum.reverse() # so we don't have to reverse the inputs/outputs on each iteration
		|> Enum.join(", ")

		placeholders = 1..length(@sql_types)
		|> Enum.map(fn _ -> "?" end)
		|> Enum.join(", ")

		concurrency = 1..4

		# MonetDB doesn't like concurrent DDL statements, so do this serially
		Enum.each(concurrency, fn i ->
			Monet.query!("drop table if exists sql_types_#{i}")
			Monet.query!("create table sql_types_#{i} (id serial, #{create})")
		end)

		fuzz = fn i ->
			insert = "insert into sql_types_#{i} (#{columns}) values (#{placeholders})"
			value_configs = Keyword.values(@sql_types)
			for _ <- 1..@fuzz_count do
				{inputs, outputs} = Enum.reduce(value_configs, {[], []}, fn config, {inputs, outputs} ->
					{i, o} = case generate(config[:type], config[:args]) do
						{i, o} -> {i, o}
						value -> {value, value}
					end
					{[i | inputs], [o | outputs]}
				end)
				assert Monet.query!(insert, inputs).row_count == 1
				[row] = Monet.query!("select #{columns} from sql_types_#{i} order by id desc limit 1").rows
				assert row == outputs
			end
		end

		concurrency
		|> Enum.map(fn i -> Task.async(fn -> fuzz.(i) end) end)
		|> Enum.map(fn t -> Task.await(t, :infinity) end)
	end

	defp generate(:utf8, {min, max}), do: nil_or(fn -> Generator.utf8(min, max) end)
	defp generate(:utf8, len), do: nil_or(fn -> Generator.utf8(len, len) end)
	defp generate(:int, power), do: nil_or(fn -> Generator.int(power) end)
	defp generate(:bool, _), do: nil_or(fn -> Generator.bool() end)
	defp generate(:float, power), do: nil_or(fn -> Generator.float(power) end)
	defp generate(:decimal, _), do: nil_or(fn -> Generator.decimal() end)
	defp generate(:date, _), do: nil_or(fn -> Generator.date() end)
	defp generate(:time, precision), do: nil_or(fn -> Generator.time(precision) end)
	defp generate(:blob, _), do: nil_or(fn -> Generator.blob() end)
	defp generate(:datetime, precision), do: nil_or(fn -> Generator.datetime(precision) end)
	defp generate(:naivedatetime, precision), do: nil_or(fn -> Generator.naivedatetime(precision) end)
	defp generate(:json, _), do: nil_or(fn -> Generator.json() end)
	defp generate(:uuid, _), do: nil_or(fn -> Generator.uuid() end)

	defp nil_or(fun) do
		case :rand.uniform(10) == 10 do
			true -> nil
			false -> fun.()
		end
	end
end
