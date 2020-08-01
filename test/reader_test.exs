defmodule Monet.Tests.Reader do
	use Monet.Tests.Base
	alias Monet.Reader

	test "prompt (an empty message)" do
		conn = start_fake_server(say: <<1, 0>>)
		assert Reader.message(conn) == {:ok, ""}
	end

	test "single frame message" do
		conn = start_fake_server(say: <<9, 0, ?d, ?u, ?n, ?e>>)
		assert Reader.message(conn) == {:ok, "dune"}
	end

	test "multi frame message" do
		conn = start_fake_server(say: <<8, 0, ?o, ?v, ?e, ?r>>, say: <<11, 0, ?9, ?0, ?0, ?0, ?!>>)
		assert Reader.message(conn) == {:ok, "over9000!"}
	end

	test "returns server error" do
		conn = start_fake_server(say: <<18, 0, ?!, ?7, ?0, ?0, ?1, ?!, ?n, ?o, ?\n>>)
		assert Reader.message(conn) == {:error, %Monet.Error{code: 7001, message: "no\n", source: :monetd}}
	end

	test "returns network error" do
		conn = start_fake_server(close: true)
		assert Reader.message(conn) == {:error, %Monet.Error{details: nil, message: :closed, source: :network}}
	end

	# These are weird MonetDB errors. I thought all errors just started with !, but
	# nope, some look like a normal result, but actually turn out to be an error.
	# I can't figure out how to reliably cause these, so we have to fake the payload
	test "handles create error" do
		conn = start_fake_server(say: <<29, 0, ?&, ?3, ?\s, ?7, ?2, ?\n, ?!, ?2, ?0, ?1, ?!, ?e, ?r, ?1>>)
		assert {:error, err} = Reader.result(conn)
		assert err.code == 201
		assert err.message == "er1"
	end
end
