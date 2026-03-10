# Backlog / future improvements

Items to revisit when we have time. Not scheduled.

---

## Dashboard: simplify dependencies (pandas + psycopg2 vs pandas + SQLAlchemy)

**Context:** The dashboard currently uses **pandas**, **psycopg2**, and **SQLAlchemy** together. All three are not strictly required.

**Current roles:**

| Dependency   | Role |
|-------------|------|
| **pandas**  | `pd.read_sql()` for runs/divergences, `pd.DataFrame`, `pd.to_datetime`, `pd.notna`; `st.dataframe()` with DataFrames. |
| **psycopg2**| Control DB: cursor-style queries (metrics, health, monitored tables, `mark_log_resolved`). Subscriber: running the repair SQL. |
| **SQLAlchemy** | `create_engine()` and `text()` so `pd.read_sql()` uses an engine (avoids pandas “DBAPI2 not tested” warning) and named params (`:run_id`) work. |

**Options to revisit:**

1. **Drop SQLAlchemy** → keep **pandas + psycopg2**
   - Use the psycopg2 connection for everything, including `pd.read_sql(conn, ...)`.
   - Use `%s` and tuple params again in SQL.
   - Accept or suppress the pandas “Other DBAPI2 objects are not tested” warning.
   - **Deps:** `pandas`, `psycopg2-binary`.

2. **Drop psycopg2** → keep **pandas + SQLAlchemy**
   - Use SQLAlchemy for Control DB and Subscriber (engine + `text()` for all queries).
   - Refactor cursor-style code to `conn.execute(text("..."), params)`.
   - SQLAlchemy still needs a driver (e.g. `psycopg2-binary`) under the hood, but the app code would not import psycopg2 directly.
   - **Deps:** `pandas`, `sqlalchemy`, and the driver.

**Decision:** Deferred. Continue with current setup for now; revisit when simplifying the dashboard stack.

---

*Add new items below.*
