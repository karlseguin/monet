defmodule Monet.Result do
  @moduledoc """
  Represents the result from a query to MonetDB.

  For a select `columns` are the column names and `rows` is a list of lists.
  These can be accessed directly. However, the module also implements Enumerable
  and Jason.Encode. By default, Enumerationa and Jason.Encode will expose the list
  of lists as-is. However, the Result can be configured to return a list of maps
  (optionally with atom keys). See `Monet.as_map/1` and `Monet.as_map/2` for
  more information.

  `last_id` is non-nil in the case of an insert to a table with an auto incremental
  column (e.g. serial) and nil in all other cases.

  `row_count` represents either the number of affected rows (for an update or
  delete) or the number of `rows` (for a select).

  Responses from the MonetDB server generally include some meta data, such as
  timing information. This data isn't useful to this driver, but it's exposed in
  in the `meta` field, in case it's useful to the caller. This data is unparsed;
  it's binary field.
  """
  alias __MODULE__

  defstruct [
    :mode,
    :meta,
    :rows,
    :last_id,
    :columns,
    :row_count
  ]

  @doc """
  Creates a new Result from a select or other queries that return data
  """
  def new(header, columns, rows, count) do
    %Result{meta: header, columns: columns, rows: rows, row_count: count}
  end

  @doc """
  Creates a new Result with only a meta field (the type of result you'd get
  from a create table, for example)
  """
  def meta(meta), do: upsert(meta, 0, nil)

  @doc """
  Creates a new Result with a count and last_id, used by update/delete/insert
  """
  def upsert(meta, count, last_id) do
    %Result{meta: meta, columns: [], rows: [], row_count: count, last_id: last_id}
  end

  @doc """
  Switches the the mode of the result to enumerate or jason encode maps. See
  `Monet.as_map/1` and `Monet.as_map/2` for more information.
  """
  def as_map(result, opts) do
    case Keyword.get(opts, :columns) == :atoms do
      false ->
        %Result{result | mode: :map}

      true ->
        %Result{result | mode: :map, columns: Enum.map(result.columns, &String.to_atom/1)}
    end
  end
end

defimpl Enumerable, for: Monet.Result do
  alias Monet.Result

  def slice(result) do
    {:ok, result.row_count, &Enum.slice(result.rows, &1, &2)}
  end

  def count(result), do: {:ok, result.row_count}
  def member?(_result, _value), do: {:error, __MODULE__}

  def reduce(_result, {:halt, acc}, _fun), do: {:halted, acc}
  def reduce(result, {:suspend, acc}, fun), do: {:suspended, acc, &reduce(result, &1, fun)}
  def reduce(%{rows: []}, {:cont, acc}, _fun), do: {:done, acc}

  def reduce(result, {:cont, acc}, f) do
    [row | rows] = result.rows
    map = create_row(result.mode, result.columns, row)
    reduce(%Result{result | rows: rows}, f.(map, acc), f)
  end

  @doc false
  # exposed so that Jason.Encoder can use it
  def create_row(:map, columns, row), do: columns |> Enum.zip(row) |> Map.new()
  def create_row(_, _columns, row), do: row
end

defimpl Jason.Encoder, for: Monet.Result do
  alias Jason.Encoder
  alias Enumerable.Monet.Result

  def encode(%{row_count: 0}, _opts), do: "[]"

  def encode(result, opts) do
    mode = result.mode
    columns = result.columns
    [row | rows] = result.rows

    first = Encoder.encode(Result.create_row(mode, columns, row), opts)

    remainder =
      Enum.reduce(rows, [], fn row, acc ->
        [acc, ',', Encoder.encode(Result.create_row(mode, columns, row), opts)]
      end)

    ['[', first, remainder, ']']
  end
end
