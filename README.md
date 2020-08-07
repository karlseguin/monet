# A MonetDB driver for Elixir

Warning: Early development.

## Usage

In your mix.exs file, add the project dependency:

```
{:monet, "~> 0.0.6"}
```

You can start a pool by adding `Monet` to your supervisor tree and providing configuration options:

```elixir
opts = [
    pool_size: 10,
    port: 50_000,
    host: "127.0.0.1",
    username: "monetdb",
    password: "monetdb",
    database: "monetdb",
    read_timeout: 10_000,
    send_timeout: 10_000,
    connect_timeout: 10_000,
]
children = [
  ...
  {Monet, opts} 
]
```

You can then use the `Monet.query/1` and `Monet.query/2` functions:

```
{:ok, result} = Monet.query("create table atreides(name text)")
{:ok, result} = Monet.query("insert into attreides (name) values (?)", ["Leto"])
```

You can optionally use the `query!` variant.

### Named Pool
When you create the pool, you have the option of providing a `name` This is useful in the case where you want to connect to multiple instances:

 ```elixir
opts = [
  pool_size: 10,
  ...
  name: :replica
]
```

When a named pool is used, the `query/2` and `query/3` functions must be used:

```elixir
{:ok, result} = Monet.query(:replica, "create table atreides(name text)")
{:ok, result} = Monet.query(:replica, "insert into atreides (name) values (?)", ["Paul"])
```

## Results
On success, a `Monet.Result` structure is returned. The `rows` field exposes a list of list.

`Monet.Result` also implements the Enumerable and Jason.Encoder protocols. By default, these simply enumerate or render `rows` as a list of lists. However, `Monet.as_map/1` can be used to change this behavior to iterate over a list of maps.

```elixir
case Monet.as_map(Monet.query("select id, name from saiyans")) do
  {:ok, result} -> ...
  {:error, err} -> ...
end
```

`as_map/1` is safe to chain with `Monet.query` as it will return any `{:error, _}` structure passed to it as-is.

Note that `result.rows` does not change. It continues to be a listof lists. What does change is the Enumerable and Jason encoding behavior.

Optionally, `columns: :atoms` can be passed to `as_map`.

## Transactions
`Monet.transaction/1` and `Monet.transaction/2` (for named pools) can be used to wrap code in a transaction:

```elixir
Monet.transaction(fn tx ->
  Monet.query!(tx, "insert into table...", [args])
  Monet.query!(tx, "select * from table")
end)
```

The function you provide can return:

* `{:rollback, value}` - to rollback the transaction and return the same 2-value tuple
* `{:commit, value}` - to commit the transaction and return `{:ok, value}`
* `{:ok, value}` - to commit the transaction and return `{:ok, value}`
* `value` - to commit the transaction and return `{:ok, value}`

## Prepared Statements
Any calls to `query` which passes arguments will use a prepared statement.

Special handling of prepared within a transaction is available. Using `Monet.prepare/3`, prepared statements can be registered with a given name and re-used. At the end of the transaction, the prepared statements are automatically deallocated.

```elixir
Monet.transaction(fn tx ->
  Monet.prepare(tx, :test_insert, "insert into test (id) values (?)")
  with {:ok, r1} <- Monet.query(tx, :test_insert, [1]),
       {:ok, r2} <- Monet.query(tx, :test_insert, [2])
  do
    {:ok, [r1, r2]}
  else
    err -> {:rollback, err}
  end
end)
```

Keep in mind that MonetDB automatically deallocates prepared statements on execution error. This is why having automatically management of prepared statements at the transaction level makes sense (since a failure to execute probably means the transaction ends). It's much more complicated at the connection level (especially when you add the indirection of the pool).
