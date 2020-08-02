defmodule Monet.Tests.Transaction do
	use Monet.Tests.Base

	setup_all do
		connect()
		Monet.query!("drop table if exists tx_test")
		Monet.query!("create table tx_test (id int)")
		:ok
	end

	test "implicit commit" do
		Monet.query!("truncate table tx_test")

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

	test "prepares transactions" do
		# Use a different pool because we want a pool_size of 1
		# This is the only way to make sure the prepared statements are cleaned up
		# after the transaction.

		connect(pool_size: 1, name: :transaction_test)
		Monet.query!("truncate table tx_test")

		Monet.transaction!(:transaction_test, fn tx ->
			Monet.prepare!(tx, :p1, "insert into tx_test (id) values (?)")
			Monet.query!(tx, :p1, [1])
			Monet.prepare!(tx, :p2, "insert into tx_test (id) values (?)")
			Monet.query!(tx, :p2, [2])
			Monet.query!(tx, :p1, [3])
		end)
		assert Monet.query!(:transaction_test, "select * from sys.prepared_statements").row_count == 0
		assert Monet.query!(:transaction_test, "select * from tx_test order by id").rows == [[1], [2], [3]]
		GenServer.stop(:transaction_test)
	end
end
