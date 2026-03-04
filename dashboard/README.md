# SyncGuard Dashboard

Streamlit UI to inspect validation runs and divergence logs from the Control database, and optionally re-run repairs on the Subscriber.

## Setup

1. **Install dashboard dependencies**

   ```bash
   pip install -e ".[dashboard]"
   # or
   pip install streamlit pandas psycopg2-binary
   ```

2. **Configure connections**

   - **Control database** (required): source for `syncguard.validation_runs` and `syncguard.divergence_log`.

   - **Subscriber database** (optional): only needed to use the "Repair" button from the UI (executes the stored repair SQL on the subscriber).

   Either set environment variables or use Streamlit secrets:

   **Environment variables**

   ```bash
   export SYNCGUARD_CONTROL_DSN="postgresql://user:pass@host:5432/control_db"
   export SYNCGUARD_SUBSCRIBER_DSN="postgresql://user:pass@host:5433/sub_db"  # optional
   ```

   **Streamlit secrets** (`.streamlit/secrets.toml`)

   ```toml
   [control_database]
   host = "localhost"
   port = 5432
   database = "control_db"
   user = "syncguard_user"
   password = "secret"

   [subscriber_database]
   host = "localhost"
   port = 5433
   database = "sub_db"
   user = "syncguard_user"
   password = "secret"
   ```

3. **Optional: `resolved_at` on divergence_log**

   To mark log entries as "resolved" after re-running a repair from the dashboard, add the column:

   ```sql
   ALTER TABLE syncguard.divergence_log
   ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP WITH TIME ZONE;
   ```

   Grant UPDATE on the table if needed:

   ```sql
   GRANT UPDATE ON syncguard.divergence_log TO syncguard_user;
   ```

## Run

From the project root:

```bash
streamlit run dashboard/app.py
```

Or with a custom port:

```bash
streamlit run dashboard/app.py --server.port 8502
```

## Features

- **Sidebar**: Filter by monitored table (from `validation_runs.table_name`).
- **Metrics**: Total checks, active divergences (unresolved), successful repairs.
- **Validation runs**: Table of runs (run_id, table_name, status, started_at, finished_at, mismatch_count).
- **Active runs**: If any run has `status = 'running'`, a status block is shown; refresh the page to update.
- **Mismatches**: Select a diverged run to see its `divergence_log` rows. Each entry can be expanded to view publisher/subscriber data and the repair SQL.
- **Repair**: With `SYNCGUARD_SUBSCRIBER_DSN` (or `subscriber_database` in secrets), a "Repair" button runs the stored SQL on the subscriber and, if the column exists, sets `resolved_at` for that log row.
