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

## Documentation

- extension guide: `docs/PG_EXTENSION.md`
- control plane: `docs/CONTROL_PLANE.md`
- grants: `docs/REQUIRED_GRANTS.md`

## Current CLI scope

The current Go CLI foundation includes:

- config loading from flags and environment variables
- PostgreSQL connectivity using `pgx`
- reads from `syncguard.bucket_catalog`
- publisher/subscriber bucket comparison
- text or JSON output
- optional control-plane inserts for `validation_runs` and `divergence_log`

The next CLI steps are drill-down, remediation planning, and packaging.

## Near-term roadmap

- add drill-down logic below mismatched buckets
- plan and execute remediation workflows from the CLI
- package the CLI for `.rpm` / `.deb` delivery
