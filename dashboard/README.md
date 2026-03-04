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
   export SYNCGUARD_VALIDATION_CMD="python main.py"  # optional; command for "Launch Full Validation"
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

- **Launch Full Validation**: Button runs `python main.py` (or `SYNCGUARD_VALIDATION_CMD` / `validation_command` in secrets) as a background subprocess. PID is stored in session state so only one run at a time. Toasts when the job starts and when it finishes.
- **Health meter**: Overall Sync % (100 - diverged_tables/total_tables) and Pending Repairs (count where `resolved_at` IS NULL or `is_resolved` is false).
- **Live execution log**: Expandable "Active Process Logs" shows the last 10 validation runs (optionally filtered by the selected run).
- **Sidebar**: Filter by monitored table (from `validation_runs.table_name`).
- **Metrics**: Total checks, active divergences, successful repairs.
- **Validation runs**: Table of runs; select a diverged run to see mismatches.
- **Repair console**: For each unresolved divergence: **Manual Review** (repair SQL in a `st.code` block, publisher/subscriber data) and **Execute Repair** (runs that SQL on the Subscriber and sets `resolved_at` / `is_resolved` in the Control DB).
- **Control DB**: All Control DB access uses a context manager so connections are closed and do not hang.
