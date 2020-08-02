# A MonetDB driver for Elixir

Warning: Early development.

## Usage

In your mix.exs file, add the project dependency:

```
{:monet, "~> 0.0.1"}
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
    connect_timeout: 10_000
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
    name: :cache
]
```

When a named pool is used, the `query/2` and `query/3` functions must be used:

```elixir
{:ok, result} = Monet.query(:cache, "create table atreides(name text)")
{:ok, result} = Monet.query(:cache, "insert into attreides (name) values (?)", ["Paul"])
```

## Transactions

`Monet.transaction/1` and `Monet.transaction/2` (for named pools) can be used to wrap code in a transaction:

```elixir
Monet.transaction(fn tx ->
  Monet.query!(tx, "insert into table...", [args])
  Monet.query!(tx, "select * from table")
end)
```

The supplied function can return `{:rollback, value}` to rollback the transaction. In such cases, `{:error, value}` will be returned.

## Prepared Statements
Any calls to `query` which passes arguments will use a prepared statement.

A future version may introduced a prepared statement cache within transactions. A global prepared statement cache is unlikely to be implemented however.
