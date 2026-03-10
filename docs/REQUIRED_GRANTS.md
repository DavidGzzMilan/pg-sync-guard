# Required database privileges (grants)

The database user used to connect to the **Publisher** and **Subscriber** for SyncGuard checks needs the following privileges.

This document covers both:

- the original **Python validator / repair** flow
- the newer **extension-backed bucket catalog** flow

## Publisher

SyncGuard only **reads** from the Publisher: it inspects catalog metadata and runs `SELECT` on the target table(s).

- **Connect** to the database.
- **Usage** on the schema(s) that contain the tables you validate.
- **Select** on those tables.

```sql
-- Replace syncguard_user, my_schema, my_table with your role and target objects.
-- Repeat GRANT USAGE / GRANT SELECT for each schema/table you will validate.

GRANT CONNECT ON DATABASE your_database TO syncguard_user;

GRANT USAGE ON SCHEMA my_schema TO syncguard_user;
GRANT SELECT ON my_schema.my_table TO syncguard_user;
```

Schema analysis reads from `pg_catalog` (`pg_index`, `pg_attribute`, `pg_class`, `pg_namespace`). Those are readable by any role that can connect; no extra grants are required.

## Subscriber

On the Subscriber, SyncGuard **reads** (same hash/segment queries as on the Publisher) and **writes** when repairing (upsert).

- **Connect** to the database.
- **Usage** on the schema(s) that contain the replicated tables.
- **Select** on those tables (for validation).
- **Insert** and **Update** on those tables (for `INSERT ... ON CONFLICT DO UPDATE` during repair).

```sql
GRANT CONNECT ON DATABASE your_database TO syncguard_user;

GRANT USAGE ON SCHEMA my_schema TO syncguard_user;
GRANT SELECT, INSERT, UPDATE ON my_schema.my_table TO syncguard_user;
```

If you only run **validation** (no repair), `SELECT` (and `USAGE`) on the Subscriber is enough; `INSERT` and `UPDATE` are only needed when using `validate_and_repair`.

## Example: one role for both servers

If the same role exists on both Publisher and Subscriber (e.g. same user for both connections):

```sql
-- On PUBLISHER (read-only for SyncGuard)
GRANT USAGE ON SCHEMA public TO syncguard_user;
GRANT SELECT ON public.replicated_table_1 TO syncguard_user;
GRANT SELECT ON public.replicated_table_2 TO syncguard_user;

-- On SUBSCRIBER (read + write for repair)
GRANT USAGE ON SCHEMA public TO syncguard_user;
GRANT SELECT, INSERT, UPDATE ON public.replicated_table_1 TO syncguard_user;
GRANT SELECT, INSERT, UPDATE ON public.replicated_table_2 TO syncguard_user;
```

## Control database (optional)

When using the [Control Plane](CONTROL_PLANE.md) (syncguard schema), the role that connects to the **control database** needs:

- **Connect** to the database.
- **Usage** on schema `syncguard`.
- **Insert**, **Select**, **Update** on `syncguard.validation_runs`.
- **Insert**, **Select**, **Update** on `syncguard.divergence_log` (Update needed for dashboard "mark resolved"), plus **Usage, Select** on the sequence `syncguard.divergence_log_log_id_seq`.

```sql
GRANT CONNECT ON DATABASE your_control_database TO syncguard_user;
GRANT USAGE ON SCHEMA syncguard TO syncguard_user;
GRANT INSERT, SELECT, UPDATE ON syncguard.validation_runs TO syncguard_user;
GRANT INSERT, SELECT, UPDATE ON syncguard.divergence_log TO syncguard_user;
GRANT USAGE, SELECT ON SEQUENCE syncguard.divergence_log_log_id_seq TO syncguard_user;
```

## Extension-side privileges

When using the PostgreSQL extension (`pg_sync_guard`) on Publisher and Subscriber, think about privileges in two parts:

1. **runtime / comparison access** for the external CLI
2. **registration / trigger setup** when calling `syncguard_register_table(...)`

### Runtime / comparison access

If the CLI only needs to read the stable bucket hashes from a database, the role needs:

- **Connect** to the database
- **Usage** on schema `syncguard`
- **Select** on:
  - `syncguard.monitored_tables`
  - `syncguard.bucket_catalog`
  - `syncguard.dirty_buckets` (optional; only if you want to inspect queue state)
  - `syncguard.worker_state` (optional; only if you want to inspect backfill state)

Example:

```sql
GRANT CONNECT ON DATABASE your_database TO syncguard_user;
GRANT USAGE ON SCHEMA syncguard TO syncguard_user;
GRANT SELECT ON syncguard.monitored_tables TO syncguard_user;
GRANT SELECT ON syncguard.bucket_catalog TO syncguard_user;
GRANT SELECT ON syncguard.dirty_buckets TO syncguard_user;
GRANT SELECT ON syncguard.worker_state TO syncguard_user;
```

If the CLI should mark buckets as reviewed through `syncguard_mark_bucket_reviewed(...)`, also grant:

```sql
GRANT UPDATE ON syncguard.bucket_catalog TO syncguard_user;
```

### Registering monitored tables

Calling `syncguard_register_table(schema_name, table_name, pk_column, bucket_size)` does more than insert config:

- writes to `syncguard.monitored_tables`
- clears prior rows in `syncguard.bucket_catalog`, `syncguard.dirty_buckets`, and `syncguard.worker_state`
- creates a trigger on the target application table

That means the role performing registration should be the **table owner** or another sufficiently privileged role. In practice, it needs:

- **Usage** on schema `syncguard`
- **Insert / Update** on `syncguard.monitored_tables`
- **Delete / Insert / Update / Select** on:
  - `syncguard.bucket_catalog`
  - `syncguard.dirty_buckets`
  - `syncguard.worker_state`
- permission to **create triggers** on the target table(s)

Example:

```sql
GRANT USAGE ON SCHEMA syncguard TO syncguard_admin;
GRANT INSERT, UPDATE ON syncguard.monitored_tables TO syncguard_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON syncguard.bucket_catalog TO syncguard_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON syncguard.dirty_buckets TO syncguard_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON syncguard.worker_state TO syncguard_admin;

-- Plus ownership / TRIGGER privilege on the target application table(s).
```

If you prefer a simpler operational model, use:

- one higher-privilege role to register tables and manage triggers
- one lower-privilege read-only role for the CLI to read bucket hashes

## Summary

| Server     | CONNECT | USAGE (schema) | SELECT (table) | INSERT (table) | UPDATE (table) |
|-----------|---------|----------------|----------------|----------------|----------------|
| Publisher | ✓       | ✓              | ✓              | —              | —              |
| Subscriber (validate only) | ✓ | ✓              | ✓              | —              | —              |
| Subscriber (validate + repair) | ✓ | ✓ | ✓              | ✓              | ✓              |
| Control (syncguard) | ✓ | ✓ (syncguard) | ✓ | ✓ | ✓ (validation_runs only) |
