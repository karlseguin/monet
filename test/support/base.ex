defmodule Monet.Tests.Base do
	use ExUnit.CaseTemplate
	import Monet.Connection, only: [connection: 1]

	using do
		quote do
			import Monet.Tests.Base
		end
	end

	def start_fake_server(opts) do
		{:ok, pid} = Monet.Tests.Server.start_link(port: 50_010)
		{:ok, socket} = :gen_tcp.connect('127.0.0.1', 50_010, [packet: :raw, mode: :binary, active: false], 1_000)
		Enum.each(opts, fn cmd -> GenServer.cast(pid, cmd) end)
		connection(socket: socket)
	end

	def get_echo() do
		receive do
			{:echo, data} -> data
		after
			500 -> raise "nothing echo'd"
		end
	end

	def connect(opts \\ []) do
		opts = Keyword.merge([
			pool_size: 3,
			port: 50_000,
			host: "127.0.0.1",
			username: "monetdb",
			password: "monetdb",
			database: "elixir_test",
		], opts)

		{:ok, _} = Monet.start_link(opts)
	end
end
