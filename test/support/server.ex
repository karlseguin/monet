defmodule Monet.Tests.Server do
  use GenServer

  alias Monet.{Reader, Writer}
  import Monet.Connection, only: [connection: 1]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    port = Keyword.fetch!(opts, :port)

    {:ok, listener} =
      :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, listener, {:continue, :loop}}
  end

  def handle_continue(:loop, listener) do
    {:ok, socket} = :gen_tcp.accept(listener)
    {:noreply, socket}
  end

  # raw, send as-is
  def handle_cast({:say, data}, socket) do
    :gen_tcp.send(socket, data)
    {:noreply, socket}
  end

  # send with a proper header
  def handle_cast({:encode, data}, socket) do
    Writer.send(connection(socket: socket), data)
    {:noreply, socket}
  end

  def handle_cast({:echo, pid}, socket) do
    res = Reader.message(connection(socket: socket))
    send(pid, {:echo, res})
    {:noreply, socket}
  end

  def handle_cast({:reply, {nil, data}}, socket) do
    # drain this message
    Reader.message(connection(socket: socket))

    Writer.send(connection(socket: socket), data)
    {:noreply, socket}
  end

  # echo + encode
  def handle_cast({:reply, {pid, data}}, socket) do
    res = Reader.message(connection(socket: socket))
    send(pid, {:echo, res})

    Writer.send(connection(socket: socket), data)
    {:noreply, socket}
  end

  def handle_cast({:close, true}, socket) do
    :gen_tcp.close(socket)
    {:noreply, nil}
  end
end
