# Required database privileges (grants)

This repository is now centered on:

- the `pg_sync_guard` PostgreSQL extension running on publisher and subscriber
- an external CLI that reads extension state and compares both sides
- an optional control-plane database for verification history

## Extension-side privileges

Think about privileges in two roles:

1. **runtime / comparison access** for the external CLI
2. **registration / trigger management** for installing monitored-table hooks

## Runtime / comparison role

If a role only needs to read the extension state from a database, it should have:

- `CONNECT` on the database
- `USAGE` on schema `syncguard`
- `SELECT` on:
  - `syncguard.monitored_tables`
  - `syncguard.bucket_catalog`
  - optionally `syncguard.dirty_buckets`
  - optionally `syncguard.worker_state`

Example:

```sql
GRANT CONNECT ON DATABASE your_database TO syncguard_reader;
GRANT USAGE ON SCHEMA syncguard TO syncguard_reader;
GRANT SELECT ON syncguard.monitored_tables TO syncguard_reader;
GRANT SELECT ON syncguard.bucket_catalog TO syncguard_reader;
GRANT SELECT ON syncguard.dirty_buckets TO syncguard_reader;
GRANT SELECT ON syncguard.worker_state TO syncguard_reader;
```

If the CLI should mark buckets as reviewed through `syncguard_mark_bucket_reviewed(...)`, also grant:

```sql
GRANT UPDATE ON syncguard.bucket_catalog TO syncguard_reader;
```

## Registration / administration role

Calling `syncguard_register_table(schema_name, table_name, pk_column, bucket_size)` does all of the following:

- upserts config into `syncguard.monitored_tables`
- clears old rows in `syncguard.bucket_catalog`
- clears old rows in `syncguard.dirty_buckets`
- resets `syncguard.worker_state`
- creates / replaces the dirty-bucket trigger on the target table
- enables that trigger as `ALWAYS`

That means the role performing registration should be the **table owner** or another sufficiently privileged administrative role.

It needs:

- `USAGE` on schema `syncguard`
- `INSERT`, `UPDATE` on `syncguard.monitored_tables`
- `SELECT`, `INSERT`, `UPDATE`, `DELETE` on:
  - `syncguard.bucket_catalog`
  - `syncguard.dirty_buckets`
  - `syncguard.worker_state`
- permission to create and alter triggers on the target application table(s)

Example:

```sql
GRANT USAGE ON SCHEMA syncguard TO syncguard_admin;
GRANT INSERT, UPDATE ON syncguard.monitored_tables TO syncguard_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON syncguard.bucket_catalog TO syncguard_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON syncguard.dirty_buckets TO syncguard_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON syncguard.worker_state TO syncguard_admin;

-- Plus ownership or equivalent trigger-management rights on the target table(s).
```

## Target table access

The extension hashes data inside the database, so the external CLI does **not** need direct `SELECT` on the application tables if it only reads the extension state.

However, the registration/admin role needs enough rights on the target tables to install the trigger.

## Control plane (optional)

If you use a separate control-plane database for verification-job history, the connecting role typically needs:

- `CONNECT` on the control database
- `USAGE` on schema `syncguard`
- `INSERT`, `SELECT`, `UPDATE` on:
  - `syncguard.validation_runs`
  - `syncguard.divergence_log`
- `USAGE`, `SELECT` on the `divergence_log` sequence if you use a `SERIAL` / sequence-backed key

Example:

```sql
GRANT CONNECT ON DATABASE your_control_database TO syncguard_cli;
GRANT USAGE ON SCHEMA syncguard TO syncguard_cli;
GRANT INSERT, SELECT, UPDATE ON syncguard.validation_runs TO syncguard_cli;
GRANT INSERT, SELECT, UPDATE ON syncguard.divergence_log TO syncguard_cli;
GRANT USAGE, SELECT ON SEQUENCE syncguard.divergence_log_log_id_seq TO syncguard_cli;
```

## Recommended operational split

A clean model is:

- `syncguard_admin`
  - installs the extension
  - registers monitored tables
  - manages triggers
- `syncguard_reader` or `syncguard_cli`
  - reads `bucket_catalog`
  - optionally marks reviewed buckets
  - writes verification history to the control plane
