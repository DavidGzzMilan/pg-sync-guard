# PostgreSQL extension (`pg_sync_guard`)

`pg_sync_guard` is the in-database hashing engine for SyncGuard's newer architecture.

It is meant to be installed on **both** Publisher and Subscriber. Each side computes and persists its own **stable per-bucket hashes** locally, and an external CLI compares those hashes across both sides.

## Design goal

The extension is optimized to avoid unnecessary work on large tables:

- It stores **stable bucket hashes** instead of a single moving table hash.
- It keeps a **dirty bucket queue** so the worker normally recomputes only the buckets touched by row changes.
- It keeps **worker state** for the initial full sweep and a statistics-based fallback rescan.
- It leaves **cross-node comparison, history, and remediation planning** to the external CLI / control plane.

## Objects created by `CREATE EXTENSION`

`CREATE EXTENSION pg_sync_guard` creates schema `syncguard` and these objects:

- `syncguard.monitored_tables`
  - Configuration per monitored table.
  - Includes `schema_name`, `table_name`, `pk_column`, and optional `bucket_size`.
- `syncguard.bucket_catalog`
  - **UNLOGGED** stable per-bucket state.
  - Stores `bucket_id`, `pk_start`, `pk_end`, `row_count`, `bucket_hash`, `dirty`, `reviewed_at`, `last_computed_at`.
- `syncguard.dirty_buckets`
  - **UNLOGGED** queue of buckets that need recomputation.
- `syncguard.worker_state`
  - **UNLOGGED** progress for the initial backfill and a `pg_stat_user_tables` fallback baseline.
- `syncguard.enqueue_dirty_bucket(...)`
  - Helper function used by the trigger path.
- `syncguard.mark_dirty_trigger()`
  - Trigger function attached to monitored tables.

## Worker model

The extension uses a **dynamic per-database background worker**:

- `CREATE EXTENSION pg_sync_guard` auto-starts a worker for that database.
- You can also manage it explicitly:
  - `SELECT syncguard_start_worker();`
  - `SELECT syncguard_worker_running();`
  - `SELECT syncguard_stop_worker();`

The worker loop processes work in this order:

1. Recompute queued buckets from `syncguard.dirty_buckets`
2. Continue the initial full backfill into `syncguard.bucket_catalog`
3. If no explicit dirty buckets exist, use `pg_stat_user_tables` as a fallback signal to trigger a table rescan

## Registering a table

Register a table with:

```sql
SELECT syncguard_register_table('public', 'my_table', 'id', 5000);
```

Parameters:

- `schema_name`
- `table_name`
- `pk_column`
- `bucket_size` (optional). If `NULL`, the extension uses `syncguard.chunk_size`.

Registration does the following:

- upserts the table into `syncguard.monitored_tables`
- resets prior bucket state for that table
- initializes `syncguard.worker_state`
- installs a row-level trigger on the target table so future changes mark buckets dirty
- enables that trigger as `ALWAYS`, so it also fires for changes applied by logical replication on the subscriber

## How bucket hashes are used

The worker computes bucket hashes in deterministic PK order using the local table data. The external CLI can then fetch rows from `syncguard.bucket_catalog` on both Publisher and Subscriber and compare them by:

- `schema_name`
- `table_name`
- `bucket_id`

If a bucket hash differs, the CLI can decide whether to:

- mark it reviewed
- drill down further
- plan a repair action
- persist job history in the control plane database

## Stable watermark verification

The current CLI can run `verify` in a lightweight `stable-watermark` mode. It uses existing extension metadata rather than a new in-database coordination protocol:

- it reads each side's current database time
- it reads `syncguard.naptime_ms`
- it computes a conservative shared cutoff in the past
- it compares only buckets where:
  - `dirty = false`
  - `last_computed_at <= shared_cutoff`

The goal is to reduce false divergences caused by sampling publisher and subscriber while dirty buckets are still being recomputed at different times.

Important limitation:

- this is a best-effort consistency window, not a strict cross-node snapshot or epoch guarantee
- buckets that are still unstable are skipped rather than treated as definitive divergences
- the CLI may report an `unstable_snapshot` if the eligible bucket set changes during repeated reads

## Example queries

### See monitored tables

```sql
SELECT *
FROM syncguard.monitored_tables
ORDER BY schema_name, table_name;
```

### See stable bucket hashes

```sql
SELECT *
FROM syncguard.bucket_catalog
ORDER BY schema_name, table_name, bucket_id;
```

### See queued dirty buckets

```sql
SELECT *
FROM syncguard.dirty_buckets
ORDER BY queued_at;
```

### Mark a bucket reviewed

```sql
SELECT syncguard_mark_bucket_reviewed('public', 'my_table', 42);
```

### Watch worker progress

```sql
SELECT now(), schema_name, table_name, bucket_id, pk_start, pk_end, row_count, dirty, last_computed_at
FROM syncguard.bucket_catalog
ORDER BY last_computed_at DESC, schema_name, table_name, bucket_id;
```

## Notes

- `bucket_catalog`, `dirty_buckets`, and `worker_state` are **UNLOGGED** because they are reconstructable and should not add unnecessary WAL pressure.
- `monitored_tables` remains logged because it is configuration and should survive crash recovery.
- The current trigger/incremental model assumes a **single integer-like primary key column** for bucket boundaries.
- A future improvement is a stronger extension-assisted verification epoch/barrier model, ideally delivered with a proper extension upgrade path via `ALTER EXTENSION UPDATE`.
