# Required database privileges (grants)

The database user used to connect to the **Publisher** and **Subscriber** for SyncGuard checks needs the following privileges.

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

## Summary

| Server     | CONNECT | USAGE (schema) | SELECT (table) | INSERT (table) | UPDATE (table) |
|-----------|---------|----------------|----------------|----------------|----------------|
| Publisher | ✓       | ✓              | ✓              | —              | —              |
| Subscriber (validate only) | ✓ | ✓              | ✓              | —              | —              |
| Subscriber (validate + repair) | ✓ | ✓ | ✓              | ✓              | ✓              |
| Control (syncguard) | ✓ | ✓ (syncguard) | ✓ | ✓ | ✓ (validation_runs only) |
