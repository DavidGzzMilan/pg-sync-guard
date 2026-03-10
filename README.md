# pg-sync-guard

`pg-sync-guard` is now an **extension-first** project for efficient logical-replication verification.

The repository currently centers on:

- `pg_sync_guard/`: a PostgreSQL extension that computes and persists **stable per-bucket hashes** inside each database
- an upcoming **external CLI**: planned to connect to publisher and subscriber, compare bucket hashes, report divergences, and optionally persist verification history in a control plane

The old Python prototype and dashboard have been removed so the repository matches the current product direction.

## Current architecture

Install `pg_sync_guard` on **both** publisher and subscriber:

- each side computes local bucket hashes
- the extension tracks dirty buckets and recomputes only the buckets affected by changes in the normal case
- an external CLI compares `syncguard.bucket_catalog` across both sides
- an optional control plane database can store verification-job history and divergence records

## Repository layout

- `pg_sync_guard/`
  - pgrx extension crate
  - dynamic per-database background worker
  - bucket catalog, dirty bucket queue, worker state, and helper SQL functions
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

## Documentation

- extension guide: `docs/PG_EXTENSION.md`
- control plane: `docs/CONTROL_PLANE.md`
- grants: `docs/REQUIRED_GRANTS.md`

## Near-term roadmap

- add the production CLI in Go
- compare bucket catalogs across publisher and subscriber
- persist verification runs and divergences in the optional control plane
- package the CLI for `.rpm` / `.deb` delivery
