defmodule Monet.Tests.Generator do
	@utf8 "test/utf8.txt" |> File.read!()  |> String.codepoints() |> List.to_tuple()
	@utf8_upper tuple_size(@utf8) - 1

	def utf8(min, max) do
		min = min - 1
		length = :rand.uniform(max - min) + min

		1..length
		|> Enum.map(fn _ -> elem(@utf8, :rand.uniform(@utf8_upper)) end)
		|> :erlang.iolist_to_binary()
	end

	def bool(), do: :rand.uniform(2) == 2

	def int(power) do
		max = trunc(:math.pow(2, power - 1) - 1)
		value = :rand.uniform(max)

		case :rand.uniform(2) == 2 do
			true -> -value
			false -> value
		end
	end

	def float(64) do
		int(63) + :rand.uniform_real()
	end

	def decimal() do
		value =  Decimal.from_float(int(31) + :rand.uniform_real())
		{value, Decimal.round(value, 3)}
	end

	def date() do
		days = :rand.uniform(3022424)
		Date.add(~D[0001-01-01], days)
	end

	def time(3) do
		ms = :rand.uniform(86399999)
		time = Time.add(~T[00:00:00], ms, :millisecond)
		{time, Time.truncate(time, :millisecond)}
	end

	def time(6) do
		ms = :rand.uniform(86399999)
		t = Time.add(~T[00:00:00], ms, :millisecond)
		{t, t}
	end

	def time(nil) do
		s = :rand.uniform(86399)
		time = Time.add(~T[00:00:00], s, :second)
		{time, Time.truncate(time, :second)}
	end

	def naivedatetime(n) do
		d = date()
		{t_in, t_out} = time(n)
		{elem(NaiveDateTime.new(d, t_in), 1), elem(NaiveDateTime.new(d, t_out), 1)}
	end

	def datetime(n) do
		{n_in, n_out} = naivedatetime(n)
		offset = :rand.uniform(20) * 1800
		offset = case :rand.uniform(2) == 2 do
			true -> -offset
			false -> offset
		end
		input = %{DateTime.from_naive!(n_in, "Etc/UTC") | utc_offset: offset}
		output = DateTime.from_naive!(NaiveDateTime.add(n_out, -offset), "Etc/UTC")
		{input, output}
	end

	def blob(), do: :crypto.strong_rand_bytes(:rand.uniform(2))
	def json(), do: Jason.encode!(%{name: utf8(1, 20), n: int(16)})
	def uuid() do
		bin = :crypto.strong_rand_bytes(16)
		<<a1::4, a2::4, a3::4, a4::4, a5::4, a6::4, a7::4, a8::4, b1::4, b2::4, b3::4, b4::4, c1::4, c2::4, c3::4, c4::4, d1::4, d2::4, d3::4, d4::4, e1::4, e2::4, e3::4, e4::4, e5::4, e6::4, e7::4, e8::4, e9::4, e10::4, e11::4, e12::4>> = bin
		<<
			e(a1), e(a2), e(a3), e(a4), e(a5), e(a6), e(a7), e(a8), ?-,
			e(b1), e(b2), e(b3), e(b4), ?-,
			e(c1), e(c2), e(c3), e(c4), ?-,
			e(d1), e(d2), e(d3), e(d4), ?-,
			e(e1), e(e2), e(e3), e(e4), e(e5), e(e6), e(e7), e(e8), e(e9), e(e10), e(e11), e(e12)
		>>
	end

	defp e(0), do: ?0
	defp e(1), do: ?1
	defp e(2), do: ?2
	defp e(3), do: ?3
	defp e(4), do: ?4
	defp e(5), do: ?5
	defp e(6), do: ?6
	defp e(7), do: ?7
	defp e(8), do: ?8
	defp e(9), do: ?9
	defp e(10), do: ?a
	defp e(11), do: ?b
	defp e(12), do: ?c
	defp e(13), do: ?d
	defp e(14), do: ?e
	defp e(15), do: ?f
end
