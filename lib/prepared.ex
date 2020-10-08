defmodule Monet.Prepared do
  @moduledoc false

  # There's nothing to like about this. Ideally, I feel that we should be able
  # to execute a prepared statement with just the id. But that isn't the case.
  # First,  need to encode some values with a prefix:
  #
  #    exec 7(23, time '01:20:33', date '2010-02-10', blob '0AF3')
  #
  # Second, we sometimes need to include the precision:
  #
  #    exec 7(23, time(3) '01:20:33.993',
  #
  # So we need to parse the response to extract the types (we can't infer it
  # from the supplies values because (a) we can't tell a text from a blob and
  # (b) we need the precision for time.
  #
  # What's even worse though is that the prepared result includes other values.
  # For example, if we do:
  #
  #    prepare "select ? - 1"
  #
  # We'll actually get 2 types back. So we need to only select the
  # 'placeholder' types.
  #
  # AFAIC, the server could take care of all of this.

  require Record
  alias Monet.{Error, Reader, Writer}

  Record.defrecord(:prepared, id: nil, conn: nil, types: nil)

  @doc """
   Parses the response from a "prepare STATMENT" query and builds up an object
   that can be executed.

   Of importance here is a) getting the id  b) getting the types
  """
  def new(conn, sql) do
    with :ok, Writer.query(conn, ["prepare ", sql]), {:ok, data} <- Reader.message(conn) do
      build(conn, data)
    end
  end

  @doc false
  def build(conn, <<"&5 ", data::binary>>) do
    case String.split(data, "\n", parts: 6) do
      # The ignored 4 lines seem pretty meaningless. I think it's to be
      # consistent with a select result. But we can parse this a little more
      # efficiently knowing that it's from a prepared statement.
      [header, _, _, _, _, rows] ->
        with {:ok, id} <- parse_id(header),
             {:ok, types} <- parse_types(rows) do
          {:ok, prepared(id: id, conn: conn, types: types)}
        end

      _ ->
        {:error, Error.new(:driver, "invalid prepared response (1)", data)}
    end
  end

  def build(_conn, invalid) do
    {:error, Error.new(:driver, "invalid prepared response (2)", invalid)}
  end

  @doc """
   Executes the prepared statement with the given argument.

   It's up to the caller to cleanup (deallocate) the prepared statement when
   they are done with it.

   Note that, on execution error, MonetDB autoamtically deallocates the prepared
   statement. This seems rather annoying, but it's up to the caller to track
   such errors and stop using (or re-prepare the query).
  """
  def exec(p, args) do
    conn = prepared(p, :conn)
    args = Writer.encode(args, prepared(p, :types))

    with :ok <- Writer.query(conn, ["exec ", prepared(p, :id), ?(, args, ?)]) do
      Reader.result(conn)
    end
  end

  @doc """
   Closes / deallocates the prepared statement.
  """
  def close(p) do
    conn = prepared(p, :conn)

    with :ok <- Writer.query(conn, ["deallocate ", prepared(p, :id)]),
         # drain the response
         {:ok, _} <- Reader.result(conn) do
      :ok
    end
  end

  @doc """
   Calls `exec/2` followed by `close/1`.
  """
  def exec_and_close(p, args) do
    result = exec(p, args)
    close(p)
    result
  end

  @doc """
  Like `exec_and_close/2` but returns both the result of the `exec/2` as well as the
  output from close/1.

  This is used by `Monet.Connection` and the Monet pool manager to figure out
  whether the connection should be returned to the pool or not.

  If `exec/2` suceeds but `close/1` errors, we'll still return the result from exec
  but we'll also remove the connection from the pool.
  """
  def exec_and_close2(p, args) do
    result = exec(p, args)
    {result, close(p)}
  end

  defp parse_id(header) do
    case :binary.split(header, " ") do
      [id, _] -> {:ok, id}
      _ -> {:error, Error.new(:driver, "invalid prepare header", header)}
    end
  end

  defp parse_types(data) do
    parse_types(data, [])
  end

  defp parse_types(<<"[ \"blob\",\t", rest::binary>>, acc) do
    case is_placeholder(rest) do
      {true, next} -> parse_types(next, [:blob | acc])
      {false, next} -> parse_types(next, acc)
    end
  end

  defp parse_types(<<"[ \"json\",\t", rest::binary>>, acc) do
    case is_placeholder(rest) do
      {true, next} -> parse_types(next, [:json | acc])
      {false, next} -> parse_types(next, acc)
    end
  end

  defp parse_types(<<"[ \"uuid\",\t", rest::binary>>, acc) do
    case is_placeholder(rest) do
      {true, next} -> parse_types(next, [:uuid | acc])
      {false, next} -> parse_types(next, acc)
    end
  end

  for type <- [:time, :timestamp, :timestamptz] do
    prefix = "[ \"#{type}\",\t"

    defp parse_types(<<unquote(prefix), rest::binary>> = data, acc) do
      case Integer.parse(rest) do
        {size, _} ->
          case is_placeholder(rest) do
            {true, next} -> parse_types(next, [{unquote(type), size - 1} | acc])
            {false, next} -> parse_types(next, acc)
          end

        :error ->
          {:error, Error.new(:driver, "invalid prepared row (#{unquote(type)})", data)}
      end
    end
  end

  defp parse_types(<<"[ \"", rest::binary>>, acc) do
    case is_placeholder(rest) do
      {true, next} -> parse_types(next, [nil | acc])
      {false, next} -> parse_types(next, acc)
    end
  end

  defp parse_types("", acc), do: {:ok, Enum.reverse(acc)}

  defp parse_types(invalid, _acc) do
    {:error, Error.new(:driver, "invalid prepared row (2)", invalid)}
  end

  # Figures out whether this is a placeholder or not and, either way, moves to
  # the next row.
  defp is_placeholder(<<rest::binary>>) do
    with [row, rest] <- :binary.split(rest, "\n") do
      placeholder? = String.ends_with?(row, "NULL,\tNULL,\tNULL\t]")
      {placeholder?, rest}
    else
      _ -> {false, rest}
    end
  end
end
