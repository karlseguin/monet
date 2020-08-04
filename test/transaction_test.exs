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

		assert {:rollback, "fail"} = Monet.transaction(fn tx ->
			Monet.query(tx, "insert into tx_test values (?)", 3)
			{:rollback, "fail"}
		end)
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

	test "returns commit error" do

		wait = fn ->
			receive do
				data -> data
			after
				100 -> raise "nothing received"
			end
		end

		pid = self()
		fun = fn id ->
			result = Monet.transaction(fn tx ->
				wait.()
				Monet.query!(tx, "insert into tx_test (id) values (1)")
				wait.()
			end)
			send(pid, {id, result})
		end

		p1 = spawn fn -> fun.(1) end
		p2 = spawn fn -> fun.(2) end
		send(p1, :ok)
		send(p2, :ok)
		send(p1, :ok)

		assert wait.() == {1, {:ok, :ok}}
		send(p2, :ok)
		assert wait.() == {2, {:error, %Monet.Error{
			code: 40000,
			details: nil,
			message: "COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead\n",
			source: :monetd
		}}}

	end
end
