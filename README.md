# pg-sync-guard

`pg-sync-guard` is now an **extension-first** project for efficient logical-replication verification.

The repository currently centers on:

- `pg_sync_guard/`: a PostgreSQL extension that computes and persists **stable per-bucket hashes** inside each database
- `cmd/syncguard-cli/`: the first Go CLI foundation for comparing publisher and subscriber bucket catalogs

The old Python prototype and dashboard have been removed so the repository matches the current product direction.

## Current architecture

Install `pg_sync_guard` on **both** publisher and subscriber:

- each side computes local bucket hashes
- the extension tracks dirty buckets and recomputes only the buckets affected by changes in the normal case
- the Go CLI compares `syncguard.bucket_catalog` across both sides
- an optional control plane database can store verification-job history and divergence records

## Repository layout

- `pg_sync_guard/`
  - pgrx extension crate
  - dynamic per-database background worker
  - bucket catalog, dirty bucket queue, worker state, and helper SQL functions
- `cmd/syncguard-cli/`
  - Go CLI entrypoint
  - currently includes MVP `verify` command scaffolding
- `internal/`
  - shared Go packages for config, DB access, extension reads, comparison, reporting, and control-plane writes
- `docs/PG_EXTENSION.md`
  - extension usage, worker model, and example queries
- `docs/CONTROL_PLANE.md`
  - optional control-plane schema for verification history
- `docs/REQUIRED_GRANTS.md`
  - grants for extension access, table registration, and control-plane access

## Extension features

- stable **per-bucket hashes** stored in `syncguard.bucket_catalog`
- **dirty bucket** queue in `syncguard.dirty_buckets`
- **dynamic worker** started per database
- initial full sweep plus incremental recomputation
- trigger-based dirty marking, including subscriber-side logical replication changes via `ENABLE ALWAYS TRIGGER`
- reconstructable runtime state stored in **UNLOGGED** tables to reduce WAL pressure

## Quick start

### 1. Build / run the extension during development

From `pg_sync_guard/`:

```bash
cargo pgrx run
```

### 2. Create the extension

```sql
CREATE EXTENSION pg_sync_guard;
```

### 3. Register a monitored table

```sql
SELECT syncguard_register_table('public', 'my_table', 'id', 5000);
```

### 4. Confirm the worker is running

```sql
SELECT syncguard_worker_running();
```

### 5. Inspect bucket hashes

```sql
SELECT *
FROM syncguard.bucket_catalog
ORDER BY schema_name, table_name, bucket_id;
```

### 6. Run the CLI verify command

With Go available locally:

```bash
go run ./cmd/syncguard-cli verify \
  --publisher-dsn "$SYNCGUARD_PUBLISHER_DSN" \
  --subscriber-dsn "$SYNCGUARD_SUBSCRIBER_DSN"
```

Optional flags:

- `--schema public`
- `--table my_table`
- `--json`
- `--control-dsn "$SYNCGUARD_CONTROL_DSN" --write-control-plane`

### 7. Inspect one mismatched bucket

After `verify` reports a mismatched bucket, drill into the affected PK window:

```bash
go run ./cmd/syncguard-cli inspect \
  --publisher-dsn "$SYNCGUARD_PUBLISHER_DSN" \
  --subscriber-dsn "$SYNCGUARD_SUBSCRIBER_DSN" \
  --schema public \
  --table my_table \
  --bucket-id 42
```

This reads `syncguard.monitored_tables` to discover the PK column and bucket size, then compares the full rows inside that bucket range on publisher and subscriber.

Note: `inspect` requires the CLI role to have direct `SELECT` on the target application table, not just on the `syncguard` schema objects.

For each row-level divergence, `inspect` now also generates a suggested repair SQL plan for the subscriber:

- `INSERT ... ON CONFLICT DO UPDATE` when the publisher row should be copied to the subscriber
- `DELETE` when the subscriber has an extra row that is missing on the publisher

### 8. Explicitly apply planned repairs

When you want to execute the suggested repair SQL against the subscriber, use `repair`:

```bash
go run ./cmd/syncguard-cli repair \
  --publisher-dsn "$SYNCGUARD_PUBLISHER_DSN" \
  --subscriber-dsn "$SYNCGUARD_SUBSCRIBER_DSN" \
  --schema public \
  --table my_table \
  --bucket-id 42
```

`repair` re-runs the bucket inspection, rebuilds the repair plan, and applies the statements to the subscriber in a single transaction. It never runs implicitly as part of `verify` or `inspect`.

Note: subscriber-side repairs fire SyncGuard's dirty-bucket trigger. In the current codebase, those helper functions are meant to run with extension-owner privileges, so the CLI repair role should only need DML rights on the target subscriber table plus the read access documented in `docs/REQUIRED_GRANTS.md`.

## Documentation

- extension guide: `docs/PG_EXTENSION.md`
- control plane: `docs/CONTROL_PLANE.md`
- grants: `docs/REQUIRED_GRANTS.md`

## Current CLI scope

The current Go CLI foundation includes:

- config loading from flags and environment variables
- PostgreSQL connectivity using `pgx`
- reads from `syncguard.bucket_catalog`
- reads from `syncguard.monitored_tables`
- publisher/subscriber bucket comparison
- bucket-level row drill-down with `inspect`
- suggested subscriber repair SQL from row-level diffs
- explicit subscriber-side apply flow with `repair`
- text or JSON output
- optional control-plane inserts for `validation_runs` and `divergence_log`

The next CLI steps are controlled execution and packaging.

## Near-term roadmap

- execute approved remediation workflows from the CLI
- package the CLI for `.rpm` / `.deb` delivery
