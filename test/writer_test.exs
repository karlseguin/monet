defmodule Monet.Tests.Writer do
	use Monet.Tests.Base
	alias Monet.Writer

	test "writes nothing" do
		conn = start_fake_server(echo: self())
		Writer.send(conn, <<>>)
		assert get_echo() == {:ok, <<>>}
	end

	test "writes single byte" do
		conn = start_fake_server(echo: self())
		Writer.send(conn, <<255>>)
		assert get_echo() == {:ok, <<255>>}
	end

	test "writes max single-frame" do
		msg = String.duplicate("1", 8190)
		conn = start_fake_server(echo: self())
		Writer.send(conn, msg)
		assert get_echo() == {:ok, msg}
	end

	test "writes max single-frame + 1" do
		msg = String.duplicate("2", 8191)
		conn = start_fake_server(echo: self())
		Writer.send(conn, msg)
		assert get_echo() == {:ok, msg}
	end

	test "writes max single-frame * 2" do
		msg = String.duplicate("3", 16380)
		conn = start_fake_server(echo: self())
		Writer.send(conn, msg)
		assert get_echo() == {:ok, msg}
	end

	test "writes max single-frame * 2 + 1" do
		msg = String.duplicate("4", 16381)
		conn = start_fake_server(echo: self())
		Writer.send(conn, msg)
		assert get_echo() == {:ok, msg}
	end

	test "writes a query" do
		conn = start_fake_server(echo: self())
		Writer.query(conn, "select power from goku")
		assert get_echo() == {:ok, "sselect power from goku;"}
	end

	test "writes a command" do
		conn = start_fake_server(echo: self())
		Writer.command(conn, "over 9000!")
		assert get_echo() == {:ok, "Xover 9000!\n"}
	end
end
