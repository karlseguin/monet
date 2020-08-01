defmodule Monet.Tests.Transaction do
	use Monet.Tests.Base

	setup_all do
		connect()
		Monet.query!("drop table if exists tx_test")
		Monet.query!("create table tx_test (id int)")
		:ok
	end

	test "implicit commit" do
		assert {:ok, result} = Monet.transaction(fn tx ->
			Monet.query!(tx, "insert into tx_test values (?)", [3])
			Monet.query(tx, "select * from tx_test")
		end)
		assert result.rows == [[3]]
		assert Monet.query!("select * from tx_test").rows == [[3]]
	end

	test "rollback" do
		Monet.query!("truncate table tx_test")

		assert {:error, result} = Monet.transaction(fn tx ->
			Monet.query(tx, "insert into tx_test values (?)", 3)
			{:rollback, "fail"}
		end)
		assert result == "fail"
		assert Monet.query!("select * from tx_test").rows == []
	end

	test "rollback on raise" do
		Monet.query!("truncate table tx_test")
		assert_raise RuntimeError, fn ->
			assert {:error, result} = Monet.transaction(fn tx ->
				Monet.query(tx, "insert into tx_test values (?)", 3)
				raise "fail"
			end)
		end
		assert Monet.query!("select * from tx_test").rows == []
	end
end
