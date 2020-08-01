defmodule Monet.Writer do
	@moduledoc """
	Prepares and sends messages to the server. Should not be called directly from
	outside this library.
	"""

	use Bitwise, only: [bsl: 2, bor: 1]
	import Kernel, except: [send: 2]  # resolve conflict
	import Monet.Connection, only: [connection: 2]

	@doc """
	Sends `data` to the server.

	MonetDB only accepts individual frames up to 8190 bytes. If our message is larger
	than this, it needs to be broken up.

	Each frame has a 2 byte header. 1 bit of the header is used to indicate if this
	is the final frame of the message or not. The rest is used for the length.
	"""
	def send(conn, data) do
		len = :erlang.iolist_size(data)
		socket = connection(conn, :socket)
		case len > 8190 do
			true ->
				header = <<252, 63>>  # max length not fin, aka: bor(bsl(8190, 1), 0)
				<<data::bytes-size(8190), rest::binary>> = :erlang.iolist_to_binary(data)
				with :ok <- do_send(socket, [header, data]) do
					send(conn, rest)
				end
			false ->
				header = <<bor(bsl(len, 1), 1)::little-16>>
				do_send(socket, [header, data])
		end
	end

	defp do_send(socket, data) do
		case :gen_tcp.send(socket, data) do
			:ok -> :ok
			{:error, err} -> {:error, Monet.Error.new(:network, err)}
		end
	end

	@doc """
	Sends a command to the server. Commands appear to be queries with just an empty
	response. This should
	"""
	def command(conn, command) do
		send(conn, ["X", command, "\n"])
	end

	@doc """
	Sends a query to the server. Except for a very few things that are considered
	"commands", almost everything is a query
	"""
	def query(conn, query) do
		send(conn, [?s, query, ?;])
	end

	@doc """
	Encodes a list of value to be sent as part of a prepare + exec flow. The types
	parameter is parsed from the response of the prepare statement. See
	Monet.Prepared for more information
	"""
	def encode(values, types, acc \\ [])
	def encode([value], [type], acc), do: [acc, encode_value(value, type)]
	def encode([value | values], [type | types], acc), do: encode(values, types, [acc, encode_value(value, type), ?,,])
	# should not be here, wrong number of values, let the server handle it
	def encode(_, _, acc), do: acc

	defp encode_value(nil, _type), do: "NULL"
	defp encode_value(f, _) when is_float(f), do: Float.to_string(f)
	defp encode_value(n, _) when is_integer(n), do: Integer.to_string(n)
	defp encode_value(%Decimal{} = d, _), do: Decimal.to_string(d)
	defp encode_value(true, _), do: "true"
	defp encode_value(false, _), do: "false"
	defp encode_value(<<data::binary>>, :blob), do: ["blob '", Base.encode16(data), ?']
	defp encode_value(<<data::binary>>, :json), do: ["json '", encode_string(data), ?']
	defp encode_value(<<data::binary>>, :uuid), do: ["uuid '", data, ?']
	defp encode_value(<<data::binary>>, _), do: [?', encode_string(data), ?']
	defp encode_value(%Time{} = t, {:time, 3}), do: ["time(3) '", Time.to_string(t), ?']
	defp encode_value(%Time{} = t, {:time, 6}), do: ["time(6) '", Time.to_string(t), ?']
	defp encode_value(%Time{} = t, _), do: ["time '", Time.to_string(t), ?']
	defp encode_value(%Date{} = t, _), do: ["date '", Date.to_string(t), ?']
	defp encode_value(%NaiveDateTime{} = t, {:time, 3}), do: ["timestamp(3) '", NaiveDateTime.to_iso8601(t), ?']
	defp encode_value(%NaiveDateTime{} = t, {:time, 6}), do: ["timestamp(6) '", NaiveDateTime.to_iso8601(t), ?']
	defp encode_value(%NaiveDateTime{} = t, _), do: ["timestamp '", NaiveDateTime.to_iso8601(t), ?']
	defp encode_value(%DateTime{} = t, {:time, 3}), do: ["timestamptz(3) '", DateTime.to_iso8601(t), ?']
	defp encode_value(%DateTime{} = t, {:time, 6}), do: ["timestamptz(6) '", DateTime.to_iso8601(t), ?']
	defp encode_value(%DateTime{} = t, _), do: ["timestamptz '", DateTime.to_iso8601(t), ?']

	def encode_string(data) do
		data
		|> String.replace("\\", "\\\\")
		|> String.replace("\'", "\\'")
	end
end
