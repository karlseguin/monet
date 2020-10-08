defmodule Monet.Auth do
  @moduledoc """
  Handles authenticating with the MonetDB server. Shouldn't be called directly.
  Automatically invoked as part of the connection startup which is managed by
  the connection pool (the Monet module).
  """

  alias Monet.{Error, Writer, Reader}

  @doc """
  The monetdbd process can either proxy or redirect us to the actual database
  server (mserver5). Even when proxying, we're required to reconnect. In either
  case, we want to limit how many times we'll attempt to do so.

  Currently, this is set to 10 (because that's what all the other drivers seem
  to do.)
  """
  def login(conn, opts, tries \\ 0)

  def login(_conn, _opts, 10), do: {:error, Error.new(:driver, "too many proxy login iterations")}

  def login(conn, opts, tries) do
    with {:ok, challenge} <- Reader.message(conn),
         {:ok, salt, auth_types, hash_algo} <- parse_challenge(challenge),
         {:ok, auth_name, auth_type} <- parse_auth_types(auth_types),
         {:ok, hash_algo} <- parse_hash_algo(hash_algo) do
      password = hash_algo |> :crypto.hash(opts[:password]) |> Base.encode16(case: :lower)
      digest = auth_type |> :crypto.hash([password, salt]) |> Base.encode16(case: :lower)

      Writer.send(conn, [
        "LIT:",
        opts[:username],
        ?:,
        auth_name,
        digest,
        ?:,
        "sql:",
        opts[:database],
        ?:
      ])

      verify(Reader.message(conn), conn, opts, tries)
    end
  end

  # login was successfull
  defp verify({:ok, ""}, conn, _opts, _tries), do: {:ok, conn}

  # monetdbd is going proxy our request to server, login again (and this time
  # monetdbd will just be proxying the request)
  defp verify({:ok, <<"^mapi:merovingian:", _::binary>>}, conn, opts, tries) do
    login(conn, opts, tries + 1)
  end

  # montdbd is redirecting us to a specific server. connect to it
  # TODO: This can be a list (\n separated, I guess we're supposed to try eac
  # one?)
  defp verify({:ok, <<"^mapi:", uri::binary>>}, _conn, _opts, _tries) do
    uri = URI.parse(uri)
    database = uri.path |> String.trim_leading("/") |> String.trim_trailing("\n")
    {:redirect, [hostname: uri.host, port: uri.port, database: database]}
  end

  defp verify({:error, _} = err, _conn, _opts, _tries), do: err

  defp parse_challenge(challenge) do
    case String.split(challenge, ":") do
      [salt, _, "9", auth_types, _endian, hash_algo, ""] -> {:ok, salt, auth_types, hash_algo}
      _ -> {:error, Error.new(:driver, "unsupported challenge response", challenge)}
    end
  end

  defp parse_auth_types(auth_types) do
    auth_types = String.split(auth_types, ",")

    cond do
      "SHA512" in auth_types -> {:ok, "{SHA512}", :sha512}
      "SHA256" in auth_types -> {:ok, "{SHA256}", :sha256}
      "SHA224" in auth_types -> {:ok, "{SHA224}", :sha224}
      "RIPEMD160" in auth_types -> {:ok, "{RIPEMD160}", :ripemd160}
      true -> {:error, Error.new(:driver, "unsupported auth_types", auth_types)}
    end
  end

  defp parse_hash_algo("SHA512"), do: {:ok, :sha512}
  defp parse_hash_algo("SHA256"), do: {:ok, :sha256}
  defp parse_hash_algo("SHA384"), do: {:ok, :sha384}
  defp parse_hash_algo("SHA224"), do: {:ok, :sha224}
  defp parse_hash_algo(unknown), do: {:error, Error.new(:driver, "unsupport pwhash", unknown)}
end
