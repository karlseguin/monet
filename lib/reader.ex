defmodule Monet.Reader do
  @moduledoc """
  Reads and parses responses from the server. Should not be called directly
  from outside this library.
  """
  use Bitwise, only: [bsr: 2, band: 1]

  import NimbleParsec
  import Monet.Connection, only: [connection: 2]

  alias Monet.{Error, Result, Prepared}

  @doc "Reads the result from a query"
  def result(conn) do
    with {:ok, payload} <- message(conn, nil) do
      parse_result(payload, conn)
    end
  end

  @doc "Reads a single message"
  def message(conn, acc \\ nil) do
    conn
    |> read_n(2)
    |> payload(conn, acc)
  end

  defp payload({:ok, <<1, 0>>}, _conn, _acc), do: {:ok, ""}

  defp payload({:ok, <<header::little-16>>}, conn, acc) do
    len = bsr(header, 1)
    fin = band(header, 1)

    case read_n(conn, len) do
      {:ok, <<"!", rest::binary>>} ->
        monet_error(rest)

      {:ok, data} ->
        cond do
          fin == 0 -> message(conn, [acc || [], data])
          acc == nil -> {:ok, data}
          true -> {:ok, :erlang.iolist_to_binary([acc, data])}
        end

      err ->
        err
    end
  end

  defp payload({:error, err}, _conn, _acc) do
    {:error, Error.new(:network, err)}
  end

  defp monet_error(<<err::binary>>) do
    {message, code} =
      case Integer.parse(err) do
        {code, <<?!, message::binary>>} -> {message, code}
        _ -> {err, nil}
      end

    {:error, %Error{source: :monetd, message: message, code: code}}
  end

  defp read_n(conn, n) do
    socket = connection(conn, :socket)
    timeout = connection(conn, :read_timeout)
    :gen_tcp.recv(socket, n, timeout)
  end

  # result from a select
  defp parse_result(<<"&1 ", data::binary>>, _conn) do
    case String.split(data, "\n", parts: 6) do
      [header, _tables, columns, types, _length, rows] ->
        with {:ok, types} <- parse_result_types(types),
             {:ok, row_count, header} <- parse_result_header(header),
             {:ok, columns} <- parse_result_columns(columns),
             {:ok, rows} <- parse_result_rows(row_count, types, rows) do
          {:ok, Result.new(header, columns, rows, row_count)}
        end

      _ ->
        {:error, Error.new(:driver, "invalid query response", data)}
    end
  end

  # result from an insert or update
  defp parse_result(<<"&2 ", data::binary>>, _conn) do
    with {row_count, <<" ", rest::binary>>} <- Integer.parse(data),
         {last_id, _} <- Integer.parse(rest) do
      {:ok, Result.upsert(data, row_count, last_id)}
    else
      _ -> {:error, Error.new(:driver, "invalid insert/update result", data)}
    end
  end

  # result from a create or drop
  defp parse_result(<<"&3 ", data::binary>>, _conn) do
    case :binary.split(data, "\n") do
      [_, <<"!", rest::binary>>] -> monet_error(rest)
      _ -> {:ok, Result.meta(String.trim_trailing(data))}
    end
  end

  # Result from a transaction. We expect it to be in auto-commit false (hence
  # the f).
  defp parse_result("&4 f\n", _conn) do
    {:ok, Result.meta("&4 f")}
  end

  # Result from a prepared request.
  defp parse_result(<<"&5 ", _::binary>> = data, conn) do
    Prepared.build(conn, data)
  end

  # Result from a QBLOCK ??
  defp parse_result(<<"&6 ", _data::binary>>, _conn) do
    raise "QBLOCK result parsing not implemented"
  end

  defp parse_result(<<unknown::binary>>, _conn) do
    {:error, Error.new(:driver, "unknown query result", unknown)}
  end

  defp parse_result_types(<<types::binary>>) do
    l = byte_size(types) - 9

    case types do
      <<"% ", types::bytes-size(l), " # type">> ->
        {:ok, types |> String.split(",\t") |> Enum.map(&String.to_atom/1)}

      _ ->
        {:error, Error.new(:driver, "invalid result type header", types)}
    end
  end

  defp parse_result_header(<<header::binary>>) do
    with [_query_id, rest] <- :binary.split(header, " "),
         {row_count, _} <- Integer.parse(rest) do
      {:ok, row_count, header}
    else
      _ -> {:error, Error.new(:driver, "invalid result header", header)}
    end
  end

  defp parse_result_columns(<<columns::binary>>) do
    l = byte_size(columns) - 9

    case columns do
      <<"% ", columns::bytes-size(l), " # name">> -> {:ok, String.split(columns, ",\t")}
      _ -> {:error, Error.new(:driver, "invalid result columns header", columns)}
    end
  end

  defp parse_result_rows(0, _types, <<_data::binary>>), do: {:ok, []}

  defp parse_result_rows(_row_count, types, <<data::binary>>) do
    do_parse_result_rows(types, data, [])
  end

  defp do_parse_result_rows(types, <<data::binary>>, acc) do
    case parse_row(types, data) do
      {:ok, "", row} -> {:ok, Enum.reverse([row | acc])}
      {:ok, rest, row} -> do_parse_result_rows(types, rest, [row | acc])
      err -> err
    end
  end

  # first value in the row, strip out the leading "[ "
  defp parse_row(types, <<"[ ", data::binary>>) do
    parse_row(types, data, [])
  end

  defp parse_row(_types, <<data::binary>>) do
    {:error, Error.new(:driver, "invalid row prefix", data)}
  end

  # last value in the row, special handling to strip out the trailing data
  defp parse_row([type], <<data::binary>>, row) do
    case parse_value(type, data) do
      {:ok, <<"\t]\n", rest::binary>>, value} -> {:ok, rest, Enum.reverse([value | row])}
      {:ok, {:text, <<"]\n", rest::binary>>}, value} -> {:ok, rest, Enum.reverse([value | row])}
      {:ok, _, _} -> {:error, Error.new(:driver, "invalid row terminator", data)}
      err -> err
    end
  end

  defp parse_row([type | types], <<data::binary>>, row) do
    case parse_value(type, data) do
      {:ok, <<",\t", rest::binary>>, value} -> parse_row(types, rest, [value | row])
      {:ok, {:text, <<rest::binary>>}, value} -> parse_row(types, rest, [value | row])
      {:ok, _, _value} -> {:error, Error.new(:driver, "invalid value separator", data)}
      err -> err
    end
  end

  defp parse_value(_type, <<"NULL", rest::binary>>), do: {:ok, rest, nil}

  defp parse_value(type, <<data::binary>>)
       when type in [:int, :tinyint, :bigint, :hugeint, :oid, :smallint, :serial] do
    case Integer.parse(data) do
      {value, rest} -> {:ok, rest, value}
      :error -> {:error, Error.new(:driver, "invalid integer", data)}
    end
  end

  defp parse_value(type, <<data::binary>>) when type in [:double, :float, :real] do
    case Float.parse(data) do
      {value, rest} -> {:ok, rest, value}
      :error -> {:error, Error.new(:driver, "invalid float", data)}
    end
  end

  defp parse_value(:decimal, <<data::binary>>) do
    case Decimal.parse(data) do
      {value, rest} -> {:ok, rest, value}
      :error -> {:error, Error.new(:driver, "invalid decimal", data)}
    end
  end

  defp parse_value(:boolean, <<"true", rest::binary>>), do: {:ok, rest, true}
  defp parse_value(:boolean, <<"false", rest::binary>>), do: {:ok, rest, false}

  defp parse_value(:boolean, invalid),
    do: {:error, Error.new(:driver, "invalid boolean", invalid)}

  @string_types [:char, :varchar, :clob, :text, :json]
  defp parse_value(type, <<?", data::binary>>) when type in @string_types do
    # Unlike the other functions, this actually strips out the trailing delimiter
    # (the "\t" or ",\t" depending on if it's the last column or not).
    # This breaks a lot of our parsing since we expect "rest" to not be consumed.
    # To solve this, and to avoid re-concatenating the separator, we return a special
    # "rest" of {:text, rest} which the other parses can special case.

    [string, rest] = :binary.split(data, "\t")
    {:ok, {:text, rest}, string |> parse_string() |> :erlang.iolist_to_binary()}
  end

  defp parse_value(type, <<invalid::binary>>) when type in @string_types do
    {:error, Error.new(:driver, "invalid string prefix", invalid)}
  end

  defp parse_value(:uuid, <<uuid::bytes-size(36), rest::binary>>) do
    {:ok, rest, uuid}
  end

  defp parse_value(:blob, <<data::binary>>) do
    {value, rest} = extract_token(data)

    case Base.decode16(value) do
      {:ok, value} -> {:ok, rest, value}
      :error -> {:error, Error.new(:driver, "invalid blob", data)}
    end
  end

  defp parse_value(:time, <<data::binary>>) do
    with {:ok, data, rest, _, _, _} <- extract_time(data),
         {:ok, time} <- build_time(data) do
      {:ok, rest, time}
    else
      _ -> {:error, Error.new(:driver, "invalid time", data)}
    end
  end

  # MonetDB strips out any leading zeros from the year, so we can't use Date.from_iso8601
  defp parse_value(:date, <<data::binary>>) do
    with {:ok, [year, month, day], rest, _, _, _} <- extract_date(data),
         {:ok, date} <- Date.new(year, month, day) do
      {:ok, rest, date}
    else
      _ -> {:error, Error.new(:driver, "invalid date", data)}
    end
  end

  defp parse_value(:timestamp, <<data::binary>>) do
    with {:ok, <<" ", rest::binary>>, date} <- parse_value(:date, data),
         {:ok, rest, time} <- parse_value(:time, rest),
         {:ok, datetime} <- NaiveDateTime.new(date, time) do
      {:ok, rest, datetime}
    else
      _ -> {:error, Error.new(:driver, "invalid timestamp", data)}
    end
  end

  # I'm pretty this timezone stuff isn't right
  defp parse_value(:timestamptz, <<data::binary>>) do
    with {:ok, <<" ", rest::binary>>, date} <- parse_value(:date, data),
         {:ok, rest, time} <- parse_value(:time, rest),
         {:ok, time_zone, rest, _, _, _} <- extract_time_zone(rest) do
      {timezone, abbreviation, offset} = build_time_zone(time_zone)

      datetime = %DateTime{
        year: date.year,
        month: date.month,
        day: date.day,
        hour: time.hour,
        minute: time.minute,
        second: time.second,
        microsecond: time.microsecond,
        utc_offset: offset,
        # ??
        std_offset: 0,
        time_zone: timezone,
        zone_abbr: abbreviation
      }

      {:ok, rest, datetime}
    else
      _ -> {:error, Error.new(:driver, "invalid timestamptz", data)}
    end
  end

  defp parse_value(type, <<data::binary>>) do
    {:error, Error.new(:driver, "unsupported type: #{type}", data)}
  end

  # We don't have to do a perfect job here, just need to figure out the boundaries.
  # The problem with :binary.split is that:
  #  a) we want to keep the separator/terminator to keep everything consistent
  #  b) the separator/terminator can be 2 different things
  defp extract_token(<<data::binary>>) do
    len = token_length(data, 0)
    <<value::bytes-size(len), rest::binary>> = data
    {value, rest}
  end

  defp token_length(<<?,, _rest::binary>>, len), do: len
  defp token_length(<<?\t, _rest::binary>>, len), do: len
  defp token_length(<<_, rest::binary>>, len), do: token_length(rest, len + 1)

  defp parse_string(<<data::binary>>, acc \\ []) do
    case :binary.split(data, "\\") do
      [text, <<?e, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\e])

      [text, <<?f, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\f])

      [text, <<?n, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\n])

      [text, <<?r, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\r])

      [text, <<?t, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\t])

      [text, <<?v, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\v])

      [text, <<?\\, rest::binary>>] ->
        parse_string(rest, [acc, text, ?\\])

      [text, <<?', rest::binary>>] ->
        parse_string(rest, [acc, text, ?'])

      [text, <<?", rest::binary>>] ->
        parse_string(rest, [acc, text, ?"])

      [text] ->
        # The last chunk can be terminated with either '"' or '",' depending
        # on whether or not it's the last column. Strip it either way.
        len1 = byte_size(text) - 1
        len2 = len1 - 1

        case text do
          <<text::bytes-size(len1), ?">> -> [acc, text]
          <<text::bytes-size(len2), ~s(",)>> -> [acc, text]
        end
    end
  end

  defp build_time([hour, minute, seconds]) do
    Time.new(hour, minute, seconds)
  end

  defp build_time([hour, minute, seconds, milli]) do
    Time.new(hour, minute, seconds, {milli * 1000, 3})
  end

  defp build_time([hour, minute, seconds, milli, micro]) do
    Time.new(hour, minute, seconds, {milli * 1000 + micro, 6})
  end

  @utc {"Etc/UTC", "UTC", 0}
  defp build_time_zone(["z"]), do: @utc
  defp build_time_zone(["Z"]), do: @utc
  defp build_time_zone(["+00:00"]), do: @utc
  defp build_time_zone(["-00:00"]), do: @utc

  defp build_time_zone([sign, hh, mm]) do
    time = "#{sign}#{hh}:#{mm}"
    hours = String.to_integer(hh)
    minutes = String.to_integer(mm)

    offset = hours * 3600 + minutes * 60

    offset =
      case sign do
        "+" -> offset
        "-" -> -offset
      end

    {"Etc/UTC" <> time, time, offset}
  end

  date =
    integer(min: 1, max: 4)
    |> ignore(string("-"))
    |> integer(2)
    |> ignore(string("-"))
    |> integer(2)

  time =
    integer(2)
    |> ignore(string(":"))
    |> integer(2)
    |> ignore(string(":"))
    |> integer(2)
    |> optional(
      ignore(string("."))
      |> integer(3)
      |> optional(integer(3))
    )

  time_zone =
    choice([
      string("z"),
      string("Z"),
      string("+00:00"),
      string("-00:00"),
      string("+") |> integer(2) |> ignore(string(":")) |> integer(2),
      string("-") |> integer(2) |> ignore(string(":")) |> integer(2)
    ])

  defparsec(:extract_date, date, inline: true)
  defparsec(:extract_time, time, inline: true)
  defparsec(:extract_time_zone, time_zone, inline: true)
end
