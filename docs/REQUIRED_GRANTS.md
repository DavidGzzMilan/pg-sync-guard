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

If the CLI should also use `inspect` to drill into a mismatched bucket, that same role additionally needs direct `SELECT` on the target application table(s), because `inspect` reads the full rows inside the bucket range.

Example for one monitored table:

```sql
GRANT SELECT ON TABLE public.sg_demo TO syncguard_reader;
```

If you want a schema-wide grant for inspection workflows:

```sql
GRANT USAGE ON SCHEMA public TO syncguard_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO syncguard_reader;
```

And if future tables in that schema should also be inspectable:

```sql
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO syncguard_reader;
```

If the CLI should also use `repair`, then on the **subscriber** it additionally needs write privileges on the target application table(s), because the repair statements are executed there:

```sql
GRANT INSERT, UPDATE, DELETE ON TABLE public.sg_demo TO syncguard_reader;
```

For broader subscriber-side repair access:

```sql
GRANT USAGE ON SCHEMA public TO syncguard_reader;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO syncguard_reader;
```

And for future subscriber tables in that schema:

```sql
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT INSERT, UPDATE, DELETE ON TABLES TO syncguard_reader;
```

Under the current extension implementation, `repair` also fires `syncguard_dirty_bucket_trigger` on the subscriber table. That trigger calls `syncguard.mark_dirty_trigger()` and `syncguard.enqueue_dirty_bucket()`, which update SyncGuard's internal state tables. So the CLI role also needs enough privilege for that trigger path on the **subscriber**:

```sql
GRANT USAGE ON SCHEMA syncguard TO syncguard_reader;
GRANT SELECT ON syncguard.monitored_tables TO syncguard_reader;
GRANT SELECT, UPDATE ON syncguard.bucket_catalog TO syncguard_reader;
GRANT SELECT, INSERT, UPDATE ON syncguard.dirty_buckets TO syncguard_reader;
```

Without those grants, repairs can fail with errors like:

```text
permission denied for table dirty_buckets
```

This extra requirement is specifically about the current trigger-based dirty-bucket implementation on the subscriber. A tighter long-term design would be to make the helper functions run with extension-owner privileges, so the CLI role would only need rights on the target application table.

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

However:

- the registration/admin role needs enough rights on the target tables to install the trigger
- the CLI role needs direct `SELECT` on the target tables if it will use `inspect` for row-level drill-down
- the CLI role needs `INSERT`, `UPDATE`, and `DELETE` on the subscriber target tables if it will use `repair`
- the CLI role also needs the trigger-path rights above on the subscriber because repairs enqueue dirty buckets

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
  - optionally inspects monitored-table rows
  - optionally applies subscriber-side repairs
  - writes verification history to the control plane
