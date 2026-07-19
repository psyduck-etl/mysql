# mysql.bulk-dedup Transformer

A high-throughput deduplication transformer that asks "which of these N values haven't I seen before?" in **one SQL query** instead of N queries. Designed for massive streams (100k+ values).

## Use Case

You have a high-volume stream of values to deduplicate against a SQL table:

```
Stream: [id=1, id=2, id=1, id=3, id=2, id=4, ...]  (100k/sec, duplicates galore)
         ↓
Dedup:   Keep only values NOT in the database
         ↓
Result:  [id=1, id=2, id=3, id=4, ...] (one copy each)
```

**Without sql-backed dedup:** In-memory dedup stores seen values in a hash set. Works fine for small streams, but at 100k/sec × seconds of traffic, memory balloons.

**With mysql.bulk-dedup:** Offload dedup to the database. Let SQL handle "which of these 100k IDs are new?". Answer comes back in one round-trip.

## How It Works

### Current Approach (Per-Record)
```sql
-- Query 1: Is id=1 already in the table?
SELECT COUNT(*) FROM seen_ids WHERE id = ?
-- Query 2: Is id=2 already in the table?
SELECT COUNT(*) FROM seen_ids WHERE id = ?
-- ... 100,000 more queries
```

**Cost:** 100,000 network round-trips (~500ms at 5ms per round-trip).

### Bulk Dedup Approach (One Query)
```sql
-- One query: Which of these 100k IDs are NOT in the table?
SELECT value FROM (VALUES (1), (2), (3), ..., (100000)) AS new_vals(value)
WHERE value NOT IN (SELECT id FROM seen_ids)
```

**Cost:** 1 network round-trip (~50ms with query execution).

**Speedup:** ~10-100× faster.

---

## Configuration

```hcl
transform "mysql.bulk-dedup" "dedup-ids" {
  connection       = "etl:etl@tcp(localhost:3306)/warehouse"
  encoding         = "json"              # How to decode incoming records
  field      = "id"                # Field name to extract from each record
  table            = "seen_ids"          # Table to check against
  table-column     = "id"                # Column in the table holding IDs
  
  # Batching strategy (required): accumulate before querying
  group-n = {
    size              = 100000          # Issue one query per 100k records
    max-query-size    = 100000          # Max values per query (prevent text bloat)
  }
  
  # OR (time-based flushing):
  group-time = {
    window = "5s"    # Issue one query every 5 seconds
  }
}
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `connection` | string | Yes | — | MySQL DSN (e.g., `user:pass@tcp(host:3306)/db`) |
| `encoding` | string | No | `json` | Codec to decode records (psyduck supports `json`, `csv`, `gzip\|json`, etc.) |
| `field` | string | Yes | — | Field name in each record holding the value to deduplicate |
| `table` | string | Yes | — | Table to check for seen values |
| `table-column` | string | Yes | — | Column in the table holding deduplicated values |
| `group-n` | object | No | — | Flush every N records (one of group-n or group-time required) |
| `group-n.size` | int | Yes* | — | Number of items to accumulate before flushing |
| `group-n.max-query-size` | int | No | `10000` | Maximum values per query (prevents query text bloat). Set 0 to disable splitting |
| `group-time` | object | No | — | Flush every T seconds (one of group-n or group-time required) |
| `group-time.window` | string | Yes* | — | Duration (e.g., `5s`, `1m`) |

---

## Example: High-Volume ID Dedup

Incoming stream: `{"id": 12345, "name": "Alice", ...}` (hundreds of thousands per second, many duplicates).

```hcl
produce "stream.events" "user-ids" {
  # Emits records with {"id": ..., "name": ..., ...}
}

transform "mysql.bulk-dedup" "dedup-users" {
  connection       = "etl:etl@tcp(localhost:3306)/warehouse"
  field      = "id"
  table            = "users_seen"
  table-column     = "user_id"
  
  group-n = {
    size              = 100000         # Every 100k records, run one query
    max-query-size    = 100000         # Allow large queries (100k values)
  }
}

consume "mysql.table" "store-new-users" {
  connection        = "etl:etl@tcp(localhost:3306)/warehouse"
  table             = "new_users"
  fields            = ["id", "name"]
  insert-chunk-size = 10000
}

pipeline "ingest-user-ids" {
  produce   = [ user-ids.events ]
  transform = [ dedup-users.dedup ]
  consume   = [ store-new-users.store ]
}
```

**Flow:**
1. Stream emits 100k records (each with an `id`).
2. `bulk-dedup` collects them.
3. When 100k are accumulated, it asks the database: "Of these 100k IDs, which ones are new?"
4. Database returns only the unseen IDs.
5. Those records flow downstream to be stored.
6. Seen IDs are silently dropped.

---

## Performance

### Latency Breakdown (100k Values)

| Phase | Per-Record (100k queries) | Bulk Query | Win |
|-------|---------------------------|-----------|-----|
| Network round-trips | 100,000 × 5ms | 1 × 50ms | **~500ms vs. 50ms** |
| Query execution | 100,000 × 1ms | 100,000 × 1ms | 0 |
| Result processing | 100,000 × 0.1ms | 100,000 × 0.1ms | 0 |
| **Total** | **~605ms** | **~105ms** | **~5-6×** |

The database still executes the check 100k times (one per value), but you've eliminated 99,999 network round-trips.

### Memory

- **Per-record:** No significant buffer needed (process immediately).
- **Bulk dedup:** Buffer holds up to `max-batch-size` records (100k = ~50-100MB for typical record size).

### Database Load

The database still scans or indexes the `table` once per query. For large tables without an index, performance can degrade. **Always index the `table-column` for dedup queries.**

---

## Correctness & Edge Cases

### Record Without Value Field

If a record is missing the `field`, an error is logged, and the record is dropped from the batch. The batch continues processing.

```hcl
# Record: {"name": "Alice"}  # Missing "id" field
# Error: "bulk-dedup: record missing field \"id\""
# Result: Record dropped, not forwarded downstream
```

### NULL Values

If a record has `id: null`, the query behavior depends on SQL NULL semantics:

```sql
SELECT value FROM (VALUES (null), (1), (2)) AS new_vals(value)
WHERE value NOT IN (SELECT id FROM seen_ids)
```

`NULL NOT IN (...)` returns `NULL` (neither true nor false), so NULL values are **filtered out** and never emitted. This is typically correct for dedup (don't emit unknown values).

### Very Large Batches (100k+)

The VALUES clause grows linearly with the number of values. At 100k values:
- Query text: ~500KB - 2MB (within MySQL's `max_allowed_packet`, typically 16-64MB).
- Parsing overhead: ~50-100ms.
- Recommendation: Keep batches ≤ 100k; use `max-batch-size` to auto-split larger batches.

### Upstream Closure

When the upstream producer closes (end of stream), any partial buffer is **automatically flushed** to the database before the transformer exits. No records are dropped.

---

## Tips & Best Practices

### 1. Index the Table Column

Always ensure the `table-column` is indexed:

```sql
CREATE TABLE seen_ids (
  id BIGINT PRIMARY KEY,
  ...
);
-- OR
CREATE INDEX idx_seen_ids ON seen_ids(id);
```

Without an index, the `NOT IN (SELECT ...)` subquery scans the entire table, defeating the batching win.

### 2. Configure group-n and max-query-size Wisely

```hcl
# For 100k/sec stream:
group-n = {
  size              = 100000  # One query every ~1 second
  max-query-size    = 100000  # Allow large queries (one flush = one query)
}

# For lower throughput (1k/sec) with smaller batches:
group-time = {
  window = "5s"  # One query every 5 seconds
}
```

Balance between:
- **`group-n.size`:** How often to flush (small = frequent flushes, large = wait for more data)
- **`group-n.max-query-size`:** How to split large flushes (small = many small queries, large = one big query)

Example trade-offs:
- `size=100k, max-query-size=100k` → One flush per 100k records → one large query
- `size=100k, max-query-size=10k` → One flush per 100k records → 10 smaller queries (more overhead, better isolation)
- `size=1k` → Frequent flushes, small buffers, rapid throughput

### 3. Memory Constraints

If your system has limited memory, reduce `group-n.size` or use `group-time` to flush more frequently:

```hcl
# Option A: Flush smaller batches
group-n = {
  size = 10000              # Smaller buffer = less memory
  max-query-size = 10000    # One query per flush
}

# Option B: Time-based flushing
group-time = { window = "1s" }  # Flush every 1 second, avoid accumulating huge buffers
```

### 4. Monitoring

Watch for:
- **Query latency:** If bulk queries start taking > 200ms, check DB load and indexes.
- **Dropped records:** Errors logged for missing fields or decode failures.
- **Memory usage:** If buffer grows unbounded, reduce `max-batch-size` or batch window.

---

## Limitations

1. **Scalar values only:** The `field` must be a single value (int, string, etc.), not a complex object or array.
2. **All-or-nothing errors:** If the query fails (e.g., DB timeout), the entire batch fails. No partial results.
3. **Correlated dedup not supported:** Can't deduplicate on multiple fields (e.g., "is this (user_id, event_type) pair new?"). Use `mysql.filter` with a custom query for that.

---

## Architecture

**Double-Buffering:**

```
Main Loop (goroutine 1)        Worker (goroutine 2)
│                              │
├─ Accumulate records ────┐    │
│                         ├─→ bufferChan (capacity 1)
│                         │    ├─ Receive buffer
│                         │    ├─ Decode + extract values
│                         │    ├─ Execute bulk query
│                         │    ├─ Emit new records
│                         │    ├─ Drop seen records
└─ (while worker ───────────→ └─ Loop back
   processes)
```

The channel has capacity 1, enabling the main loop to fill buffer B while the worker processes buffer A. This overlaps I/O with accumulation, reducing overall latency.

---

## See Also

- `mysql.filter` — Per-record filtering with arbitrary SQL predicates.
- `mysql.table` — Batch INSERT/UPSERT consumer.
- `mysql.query` — Producer that reads from the database.
