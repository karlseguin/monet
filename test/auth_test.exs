defmodule Monet.Tests.Auth do
  use Monet.Tests.Base
  alias Monet.Auth

  test "accepted login" do
    conn =
      start_fake_server(
        encode: "oRzY7XZr1EfNWETqU6b2:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:",
        reply: {self(), ""}
      )

    assert {:ok, _} = Auth.login(conn, username: "leto", password: "atreides", database: "dune")

    assert get_echo() ==
             {:ok,
              "LIT:leto:{SHA256}9f133d2ccda31b36cb9c4a848cf4332635d353b5c8c0fee341a8c90ffcc38127:sql:dune:"}
  end

  test "proxy login" do
    conn =
      start_fake_server(
        encode: challenge(),
        reply: {nil, "^mapi:merovingian://proxy?database=caladan\n"},
        encode: "766ff6hj7:merovingian:9:RIPEMD160,SHA1,MD5:LIT:SHA256:",
        reply: {self(), ""}
      )

    assert {:ok, _conn} =
             Auth.login(conn, username: "duncan", password: "idaho", database: "caladan")

    assert get_echo() ==
             {:ok, "LIT:duncan:{RIPEMD160}0570a97a0657bea37be47e8383b40a81cd78c8b4:sql:caladan:"}
  end

  test "redirect login" do
    conn =
      start_fake_server(
        encode: challenge(),
        reply: {nil, "^mapi:monetdb://caladan.dune.local:50001/dune_db\n"}
      )

    assert {:redirect, uri} =
             Auth.login(conn, username: "duncan", password: "idaho", database: "caladan")

    assert uri[:port] == 50001
    assert uri[:hostname] == "caladan.dune.local"
    assert uri[:database] == "dune_db"
  end

  defp challenge() do
    "oRzY7XZr1EfNWETqU6b2:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:"
  end
end
