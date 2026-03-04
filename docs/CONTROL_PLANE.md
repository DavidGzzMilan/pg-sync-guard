# Control Plane (syncguard schema)

SyncGuard can write run metadata and divergence details to a **central control database** using a **separate connection**. This keeps an audit trail of validations and repairs without touching the Publisher/Subscriber connections used for hashing.

## Schema DDL

Create the following in your control database (can be the same host as Publisher/Subscriber or a dedicated host):

```sql
CREATE SCHEMA IF NOT EXISTS syncguard;

-- Tracks every time SyncGuard runs a check
CREATE TABLE syncguard.validation_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name TEXT NOT NULL,
    status TEXT DEFAULT 'running',  -- running, success, diverged, failed
    started_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    finished_at TIMESTAMP WITH TIME ZONE,
    mismatch_count INT DEFAULT 0
);

-- Logs each diverged row and the applied fix
CREATE TABLE syncguard.divergence_log (
    log_id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES syncguard.validation_runs(run_id),
    pk_value TEXT NOT NULL,
    publisher_data JSONB,
    subscriber_data JSONB,
    repair_sql TEXT,
    repaired_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE  -- set when user marks as resolved from dashboard (e.g. after re-running repair)
);

-- If you already have divergence_log without resolved_at:
-- ALTER TABLE syncguard.divergence_log ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP WITH TIME ZONE;
```

## Required grants (control database)

The role used to connect to the control DB needs:

```sql
GRANT USAGE ON SCHEMA syncguard TO syncguard_user;
GRANT INSERT, SELECT, UPDATE ON syncguard.validation_runs TO syncguard_user;
GRANT INSERT, SELECT, UPDATE ON syncguard.divergence_log TO syncguard_user;
GRANT USAGE, SELECT ON SEQUENCE syncguard.divergence_log_log_id_seq TO syncguard_user;
```

## Usage

Use a **separate connection** for the control database. Pass it as `control_conn` to `validate_and_repair`; SyncGuard will:

1. **At start**: insert a row into `validation_runs` with `status = 'running'`.
2. **For each repaired row**: insert into `divergence_log` (pk_value, publisher_data, subscriber_data, repair_sql, repaired_at).
3. **At end**: update the run with `status` ('success' or 'diverged'), `finished_at`, and `mismatch_count`.

Example:

```python
import asyncio
import asyncpg
from sync_guard import SyncGuard, validate_and_repair, console_progress

async def main():
    pub = await asyncpg.connect(PUB_DSN)
    sub = await asyncpg.connect(SUB_DSN)
    control = await asyncpg.connect(CONTROL_DSN)  # separate connection

    guard = SyncGuard(pub, sub)
    repaired = await validate_and_repair(
        guard, "public", "customers",
        control_conn=control,
        progress_callback=console_progress,
    )

    await pub.close()
    await sub.close()
    await control.close()

asyncio.run(main())
```

- **control_conn**: connection to the database where the `syncguard` schema exists.
- **progress_callback**: optional `(stage, detail)` callback for human-readable output; `console_progress` prints to stdout with a `[SyncGuard]` prefix.

You can also pass a pre-built `ControlPlane` instance instead of `control_conn`:

```python
from sync_guard import ControlPlane

control = await asyncpg.connect(CONTROL_DSN)
cp = ControlPlane(control, schema="syncguard")
repaired = await validate_and_repair(guard, "public", "customers", control_plane=cp)
```

## Run statuses

| status   | Meaning |
|----------|---------|
| running  | Run started, not yet finished |
| success  | Finished; no hash mismatches (or no repairs needed) |
| diverged | Finished; one or more rows were repaired |
| failed   | Run ended with an exception |
