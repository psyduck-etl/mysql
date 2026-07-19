# psyduck mysql plugin

A [MySQL](https://dev.mysql.com/) plugin for
[Psyduck](https://github.com/psyduck-etl/sdk), aimed at ingesting snapshots of
external resources. It exposes three resources:

- `mysql.table` — a **consumer** that batch-loads records into a table, and can
  create the table from a schema if it doesn't exist yet;
- `mysql.filter` — a **transformer** that asks the database a yes/no question
  about each record (does it already exist? is it within some bound?) and
  passes or drops it accordingly;
- `mysql.query` — a **producer** that runs a query and emits each result row as
  a record, reading a worklist back out of the database.

Here a **snapshot** is a single record capturing some external resource — a
user profile, a post — at a point in time. Each captured record is appended as
its own row, so re-capturing the same entity later just adds another row
representing it at that later moment.

Built against `github.com/psyduck-etl/sdk` (see `go.mod` for the exact
version). Runs as a gRPC subprocess (`sdk/rpc`) launched by the host — no
`-buildmode=plugin`, no toolchain matching required.

```sh
go build -o mysql .
```

---

## Writing Psyduck pipelines

Psyduck pipelines are written in HCL (`.psy` files) out of **mover** blocks
(configured resources) and a **pipeline** block that wires them together.

### Mover blocks

```hcl
<role> "<plugin>.<resource>" "<instance>" {
  # host-owned block metadata (optional, works on any mover)
  per-minute = 180   # rate limit; 0 = unrestricted
  stop-after = 0     # stop after n items; 0 = unbounded

  # resource-specific configuration
  <field> = <value>
  ...
}
```

- The block type is the role: `produce`, `consume`, or `transform`.
- The **first label** `"<plugin>.<resource>"` selects the resource, e.g.
  `"mysql.table"` or `"mysql.filter"`.
- The **second label** is an instance name you choose; refer to the mover
  elsewhere as `<resource>.<instance>` (e.g. `table.load`).

### The pipeline block

```hcl
pipeline "<name>" {
  produce   = [ queue.orders-in ]
  transform = [ filter.dedup ]
  consume   = [ table.load ]
}
```

Each attribute is a list, so one pipeline can fan several producers through a
transform chain into several consumers.

### Record shape

Records flow between movers as bytes. `mysql.table` and `mysql.filter` decode
each record with the `encoding` codec into a field→value map, then use
`fields` to pick columns by name. A JSON producer upstream should therefore
emit objects like `{"order_id": 7, "customer": "acme"}`.

The `encoding` value is a codec spec resolved by whichever factory the host
binary registered at startup. Under psyduck, that's the stdlib's chain
resolver, so `"json"`, `"csv"`, and chains like `"gzip|json"` all work; a
different host could register any factory that satisfies `sdk.CodecFactory`.
Values are case-insensitive, so a legacy `"JSON"` still resolves.

---

## Resource: `mysql.table` (consumer)

Batch-loads decoded records into a table.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | string | *(required)* | DSN, e.g. `user:pass@tcp(host:3306)/db` |
| `table` | string | *(required)* | Destination table |
| `fields` | list(string) | *(required)* | Columns to write, in order; picked from each record by name |
| `encoding` | string | `json` | Codec spec resolved via the host's registered factory (psyduck: `json`, `csv`, chains like `gzip\|json`) |
| `insert-chunk-size` | int | `1` | Rows buffered per `INSERT`. Records accumulate and flush as one multi-row statement per chunk, and again on close |
| `write-mode` | string | `insert` | `insert` (fail on a unique-key collision), `insert-ignore` (silently skip collisions), `replace`, or `upsert` (`INSERT … ON DUPLICATE KEY UPDATE`) |
| `schema` | string | `""` | Column/constraint definitions. When set, the plugin runs `CREATE TABLE IF NOT EXISTS <table> (<schema>)` before consuming. **Trusted, author-supplied config only** |

### Chunked / batched loading

`insert-chunk-size` turns per-row inserts into batched multi-row inserts —
the main throughput lever for bulk ingest:

```hcl
consume "mysql.table" "load" {
  connection        = "etl:etl@tcp(localhost:3306)/warehouse"
  table             = "orders"
  fields            = ["order_id", "customer", "total"]
  insert-chunk-size = 1000     # one INSERT per 1000 records
  write-mode        = "upsert" # keep the latest version of each order
}
```

### Ensuring the table exists

If the destination table might not exist, give it a `schema` — the column and
constraint definitions that go inside the `CREATE TABLE (...)`. The plugin
issues `CREATE TABLE IF NOT EXISTS <table> (<schema>)` once, before consuming,
so the load never fails on a missing table:

```hcl
consume "mysql.table" "capture-posts" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  table      = "post_snapshots"
  fields     = ["post_id", "body", "captured_at"]
  schema     = <<-SQL
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    post_id     BIGINT NOT NULL,
    body        TEXT,
    captured_at TIMESTAMP NOT NULL,
    INDEX (post_id, captured_at)
  SQL
}
```

Each incoming record is appended as its own row — capturing the same
`post_id` again later adds another snapshot with a new `captured_at`. `schema`
is interpolated as-is, so treat it as trusted pipeline config, never
record-derived data.

---

## Resource: `mysql.filter` (transformer)

Runs one SQL query per record and passes the record through unchanged when the
query's scalar result equals `pass-when` (compared as text); otherwise the
record is dropped. All the predicate logic lives in the query, so a single
resource covers existence checks, de-duplication, recency windows, referential
gates, and anything else you can express in SQL.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | string | *(required)* | DSN |
| `query` | string | *(required)* | SQL query returning a single scalar. Reference record fields as `:name` placeholders — they are bound as parameters, never interpolated. **Trusted, author-supplied config** |
| `pass-when` | string | `1` | The record passes when the query's scalar result equals this value (as text); otherwise it is dropped |
| `encoding` | string | `json` | Codec spec used to decode the incoming record before binding `:name` placeholders |

Record fields are referenced as `:name` and bound safely as query parameters —
the record's *values* never touch the SQL text, only the query you write does.

### De-duplication (don't ingest twice)

Keep only records not already in the target table:

```hcl
transform "mysql.filter" "dedup" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  query      = "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = :order_id)"
  pass-when  = "0"   # EXISTS returns 0 when the order is new -> pass it
}
```

### Bounded criteria (don't re-scan recent sources)

"Have we scanned this source in the last hour?" — drop records whose source was
touched inside the window:

```hcl
transform "mysql.filter" "skip-recent" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  query      = <<-SQL
    SELECT EXISTS(
      SELECT 1 FROM scan_log
      WHERE source = :source AND scanned_at > NOW() - INTERVAL 1 HOUR
    )
  SQL
  pass-when  = "0"   # 0 = not scanned recently -> pass
}
```

### Referential gate (only ingest known entities)

Pass only records that match an existing row — e.g. events for customers you
already know about:

```hcl
transform "mysql.filter" "known-customers" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  query      = "SELECT EXISTS(SELECT 1 FROM customers WHERE customer_id = :customer_id)"
  pass-when  = "1"   # 1 = customer exists -> pass
}
```

Because the whole query is yours, `pass-when` isn't limited to `0`/`1` — a
query returning a status column, a count, or any scalar can gate on its exact
value.

---

## Resource: `mysql.query` (producer)

Runs one SQL query when the pipeline starts and emits each result row as a
record — columns mapped to a `{column: value}` object. It's the read
counterpart to `mysql.table`: pull a filtered, ordered worklist back out of the
database and feed it downstream (into a queue, a lookup, another pipeline).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | string | *(required)* | DSN |
| `query` | string | *(required)* | SQL query run once at startup; each result row becomes one record, columns mapped to fields. Parameterize via HCL `value`/`env` interpolation, not runtime binding. **Trusted, author-supplied config** |
| `encoding` | string | `json` | Codec spec used to encode each emitted row |

Driver-native `[]byte` cells (how MySQL hands back text and blob columns) are
rendered as strings, so JSON output isn't base64-encoded.

```hcl
produce "mysql.query" "pending-tags" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  query      = <<-SQL
    SELECT tag FROM tags
    WHERE searched = 0
    ORDER BY seen DESC
    LIMIT 100
  SQL
}
```

Each row emits a record like `{"tag": "cats"}` for the next stage to act on —
e.g. a queue of tags to go search.

---

## End-to-end: AMQP → dedup → MySQL

Paired with [`psyduck-etl/amqp`](https://github.com/psyduck-etl/amqp): read
order events off a queue, drop ones already loaded, batch-insert the rest.

```hcl
produce "amqp.queue" "orders-in" {
  connection = "amqp://guest:guest@localhost:5672"
  queue      = "orders.in"
  prefetch   = 200
  durable    = true
}

transform "mysql.filter" "dedup" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  query      = "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = :order_id)"
  pass-when  = "0"
}

consume "mysql.table" "load" {
  connection        = "etl:etl@tcp(localhost:3306)/warehouse"
  table             = "orders"
  fields            = ["order_id", "customer", "total"]
  insert-chunk-size = 500
  write-mode        = "upsert"
}

pipeline "ingest-orders" {
  produce   = [ queue.orders-in ]
  transform = [ filter.dedup ]
  consume   = [ table.load ]
}
```
