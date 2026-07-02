# psyduck mysql plugin

A [MySQL](https://dev.mysql.com/) plugin for
[Psyduck](https://github.com/psyduck-etl/sdk), aimed at snapshot-style
ingestion from external sources. It exposes two resources:

- `mysql.table` — a **consumer** that batch-loads records into a table, with
  optional transactional snapshot semantics;
- `mysql.filter` — a **transformer** that asks the database a yes/no question
  about each record (does it already exist? is it within some bound?) and
  passes or drops it accordingly.

Built against `github.com/psyduck-etl/sdk` **v0.5.0**.

```sh
go build -buildmode=plugin -o mysql.so .
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
each record with the `encoding` codec (only `JSON` today) into a field→value
map, then use `fields` to pick columns by name. A JSON producer upstream
should therefore emit objects like `{"order_id": 7, "customer": "acme"}`.

---

## Resource: `mysql.table` (consumer)

Batch-loads decoded records into a table.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | string | *(required)* | DSN, e.g. `user:pass@tcp(host:3306)/db` |
| `table` | string | *(required)* | Destination table |
| `fields` | list(string) | *(required)* | Columns to write, in order; picked from each record by name |
| `encoding` | string | `JSON` | Record codec (only `JSON` today) |
| `insert-chunk-size` | int | `1` | Rows buffered per `INSERT`. Records accumulate and flush as one multi-row statement per chunk, and again on close |
| `write-mode` | string | `insert-ignore` | `insert-ignore` (skip key collisions), `insert` (fail on collision), `replace`, or `upsert` (`INSERT … ON DUPLICATE KEY UPDATE`) |
| `snapshot` | bool | `false` | Run the whole load in one transaction, committing only on a clean finish. The table never exposes a partial load |
| `truncate` | bool | `false` | With `snapshot`, `TRUNCATE` the table at the start of the transaction so the load fully replaces prior contents |

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

### Snapshot loading

To ingest an external dataset as an all-or-nothing point-in-time snapshot,
wrap the load in a transaction and replace the table's contents:

```hcl
consume "mysql.table" "snapshot" {
  connection        = "etl:etl@tcp(localhost:3306)/warehouse"
  table             = "customers"
  fields            = ["id", "name", "email"]
  insert-chunk-size = 1000
  snapshot          = true   # commit once, at the end
  truncate          = true   # full replace: clear first, inside the txn
}
```

If anything fails partway, the transaction rolls back and the previous
contents remain visible — readers never see a half-loaded table.

---

## Resource: `mysql.filter` (transformer)

Probes the database for each record and passes it through unchanged or drops
it. The question is: *does a row exist in `table` matching every one of
`fields` (equality against the record's values) and satisfying the optional
`filter-sql` clause?*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | string | *(required)* | DSN |
| `table` | string | *(required)* | Table to probe |
| `fields` | list(string) | *(required unless `filter-sql` set)* | Record fields matched by equality against columns of the same name |
| `encoding` | string | `JSON` | Record codec |
| `pass-when` | string | `absent` | `absent`: pass records with **no** matching row (de-duplication); `present`: pass only records that **do** match |
| `filter-sql` | string | `""` | Trusted SQL predicate ANDed onto the probe, e.g. a recency window. **Author-supplied config only — never interpolate record data here** |

### De-duplication (don't ingest twice)

`pass-when = "absent"` keeps only records that aren't already in the target
table — an idempotent ingest guard:

```hcl
transform "mysql.filter" "dedup" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  table      = "orders"
  fields     = ["order_id"]
  pass-when  = "absent"
}
```

### Bounded criteria (don't re-scan recent sources)

`filter-sql` adds a trusted predicate, so you can express "have we scanned
this source recently?" and skip records whose source was touched in the last
hour:

```hcl
transform "mysql.filter" "skip-recent" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  table      = "scan_log"
  fields     = ["source"]
  filter-sql = "scanned_at > NOW() - INTERVAL 1 HOUR"
  pass-when  = "absent"   # drop sources scanned within the window
}
```

### Referential gate (only ingest known entities)

`pass-when = "present"` inverts the check — pass only records that match an
existing row, e.g. events for customers you already know about:

```hcl
transform "mysql.filter" "known-customers" {
  connection = "etl:etl@tcp(localhost:3306)/warehouse"
  table      = "customers"
  fields     = ["customer_id"]
  pass-when  = "present"
}
```

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
  table      = "orders"
  fields     = ["order_id"]
  pass-when  = "absent"
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
