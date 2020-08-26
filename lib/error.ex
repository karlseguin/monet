defmodule Monet.Error do
	@moduledoc """
	Represents an error.

	The `source` field is `:monetd` when the error was returned by the MonetDB server.
	In such cases the `code` field should be the integer code returned (though it
	could be `nil` in the unlikely case that the error couldn't be parsed properly).

	The `source` field is `client` when Monet.row, Monet.row!, Monet.map,
	Monet.map!, Monet.scalar or Monet.scalar! are called on a result with more
	rows or columns than is expected (e.g., calling Monet.scalar on a result that
	has more than 1 row or more than 1 column).Monet

	Otherwise the `source` field can be either `:network` or `:driver` to indicate a
	tcp-level error or an error arising from this library. In both cases, `code`
	will always be nil.

	The `message` field contains a human readable description of the problem. It is
	always present. It's usually a string, except when `source` is `:tcp` it will
	be an atom.

	The `details` field can contain anything, including `nil`. This is generally
	set by `Monet.Reader` on a parsing error to provide some context about the
	data which could not be parsed.
	"""

	alias __MODULE__

	defexception [
		:source,
		:message,
		:details,
		:code,
	]

	@doc """
	Turns an Monet.Error into a binary for display
	"""
	def message(e) do
		:erlang.iolist_to_binary([
			Atom.to_string(e.source), ?\s,
			to_string(e.message),  # can be an atom
			details(e.details)
		])
	end

	@doc false
	def new(source, message) do
		%Error{source: source, message: message, details: nil}
	end

	@doc false
	def new(source, message, <<details::binary>>) do
		%Error{source: source, message: message, details: details}
	end

	@doc false
	def closed?(%{source: :network}), do: true
	def closed?(_), do: false

	defp details(nil), do: []
	defp details(details), do: ["\n\n" | inspect(details)]
end
