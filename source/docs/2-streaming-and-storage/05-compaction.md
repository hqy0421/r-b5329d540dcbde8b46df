# Compaction

`pg-cdc compact` merges delta files into a new base snapshot. This reduces file count, speeds up consumer refresh, and manages tombstones.

## What Happens

```
pg-cdc compact
  For each active table:
    1. Read current base Parquet into memory (keyed by PK)
    2. Read all delta files since base_epoch
    3. Apply changes in epoch order:
       - I (insert): add row
       - U (update): replace row by PK
       - D (delete): mark row with __deleted_at tombstone
       - T (truncate): tombstone every row still visible at this boundary
    4. Purge tombstones older than 30 days
    5. Write new base Parquet
    6. Update manifest (bump base_epoch, row_count)
    7. Delete old delta files
```

## TRUNCATE semantics

A `T` row in a delta means the whole relation was emptied at that epoch,
not that one row changed. Compaction therefore tombstones **all** rows
present in the base at that point, stamping them with `__deleted_at`. Two
properties make the daily reload safe:

- **Re-inserts survive.** Any I/U after the `T` boundary is applied on top
  and clears any stale tombstone on that PK, so freshly imported data stays
  live and is never later swept by the 30-day purge.
- **History stays readable.** Each compaction writes a *new* base file
  (`base_000001.parquet`, `base_000002.parquet`, …) and never deletes an
  older one. A manifest captured before the truncation still points at the
  pre-truncation base, so an old consumer manifest keeps seeing yesterday's
  rows while the current base shows only what survived the boundary.

The streamer runs this compaction inline whenever a flush contains a `T`
boundary, so the base never serves rows that Postgres has already
truncated. `pg-cdc compact` over the same deltas is idempotent and reaches
the identical result if the inline run ever fails.

## Usage

```bash
pg-cdc compact --config pg-cdc.yml
```

## When to Compact

- **After accumulating many deltas** — hundreds of small files slow down consumer refresh
- **On a schedule** — hourly or daily via cron
- **Before a consumer's first refresh** — fewer files to download

## Tombstones

When a row is deleted, compaction doesn't remove it immediately. Instead, it marks it with `__deleted_at`:

```
Row: {id: 50, name: "...", __deleted_at: "2026-04-12T14:00:00Z"}
```

This ensures consumers who haven't refreshed recently can still propagate the delete. Tombstones are purged after 30 days (configurable).

If a consumer is offline longer than the tombstone retention period, their next `pg-warehouse refresh` detects the gap (local epoch < base_epoch) and does a full re-pull from the latest base.

## Governance During Compaction

Compaction only processes tables in `manifest.ActiveTables()`. Excluded tables are untouched. Their manifest entries (status=excluded, tags, reason) are preserved.

## Output

```
pg-cdc compact
  public.orders: compacted 15 deltas → base_000015 (10042 rows, 18 applied, 0 purged)
  public.customers: up to date (base=0, latest=0)
  public.products: compacted 3 deltas → base_000003 (100 rows, 5 applied, 1 purged)
Compaction complete
```
