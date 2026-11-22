# Streaming

`pg-cdc start` connects to the replication slot and streams WAL changes into delta Parquet files.

## What Happens

```
pg-cdc start
  1. Read manifest.json for init LSN and active table list
  2. Connect to replication slot at init LSN
  3. Decode pgoutput v2 WAL messages (INSERT/UPDATE/DELETE/TRUNCATE)
  4. Buffer events in memory
  5. Flush to delta Parquet on interval or row count
  6. Update manifest.json (bump latest_delta_epoch)
  7. If the flush contained a TRUNCATE boundary, compact the affected table(s) immediately
  8. Send standby status to PostgreSQL (prevent WAL accumulation)
```

## Usage

```bash
pg-cdc start --config pg-cdc.yml
```

Runs as a long-lived daemon. Stop with SIGINT (Ctrl+C) or SIGTERM.

## Delta Parquet Format

Each delta file contains CDC events with two metadata columns:

| Column | Type | Values |
|--------|------|--------|
| `__op` | UTF8 | `I` (insert), `U` (update), `D` (delete), `T` (truncate) |
| `__epoch` | INT64 | Epoch number |
| *(table columns)* | typed | PostgreSQL values (NULL for D and T ops except PK) |

## TRUNCATE support

A daily job pattern like `TRUNCATE staging; COPY staging FROM ...` is a
first-class event. PostgreSQL only emits TRUNCATE over logical replication
when the publication lists it, so `init` creates (or upgrades) the
publication with `WITH (publish = 'insert, update, delete, truncate')`.

A TRUNCATE WAL message carries **no rows** — just the relations it emptied
(one statement may cover several tables). The decoder turns it into a `T`
delta row whose only populated columns are `__op`/`__epoch`; every data
column is NULL. This row is the **truncation boundary** for that table.

Because the boundary must not wait for the next scheduled `compact`, the
streamer compacts the affected table as part of the same flush that
persists the `T` delta. Compaction tombstones every row visible before the
boundary (they still age out under the normal 30-day tombstone TTL), while
rows inserted **after** the boundary — the re-import — are applied on top
and stay visible. If that in-line compaction fails, the boundary delta is
already durable and a later `pg-cdc compact` converges to the same state.

## Epochs

An epoch is an atomic batch of changes. Each flush creates one epoch file:

```
public.orders/deltas/epoch=000001.parquet   ← first batch
public.orders/deltas/epoch=000002.parquet   ← second batch
```

Epochs are monotonically increasing. pg-warehouse uses epoch numbers as watermarks to track what it has already pulled.

## Flush Behavior

```yaml
flush:
  interval_sec: 10    # flush every 10 seconds
  max_rows: 1000      # or every 1000 rows
```

Whichever threshold is hit first triggers a flush. On graceful shutdown, remaining buffered events are flushed.

## Governance During Streaming

The WAL stream is scoped by the PostgreSQL publication (created during init). Only tables included at init time will appear in the stream. As a safety net, the streaming loop also checks each event's table against `manifest.ActiveTables()` and skips unknown tables.

## Monitoring

While streaming, check health with:

```bash
pg-cdc status --config pg-cdc.yml
```
