"""
SyncGuard Dashboard — Asynchronous control center for SyncGuard.

- Subprocess execution for full validation (python main.py in background).
- Health meter (Overall Sync %, Pending Repairs), live execution log.
- Repair console with Manual Review and Execute Repair; all Control DB access
  via context manager. Toasts for job start/finish.
"""

import json
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st
from sqlalchemy import text

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

SCHEMA = "syncguard"


def get_control_dsn() -> str:
    """Control DB connection string from Streamlit secrets or env."""
    if hasattr(st, "secrets") and st.secrets.get("control_database"):
        c = st.secrets["control_database"]
        return f"postgresql://{c.get('user')}:{c.get('password')}@{c.get('host')}:{c.get('port', 5432)}/{c.get('database')}"
    return os.environ.get("SYNCGUARD_CONTROL_DSN", "")


def get_subscriber_dsn() -> str:
    """Subscriber DB connection string (for executing repairs)."""
    if hasattr(st, "secrets") and st.secrets.get("subscriber_database"):
        c = st.secrets["subscriber_database"]
        return f"postgresql://{c.get('user')}:{c.get('password')}@{c.get('host')}:{c.get('port', 5432)}/{c.get('database')}"
    return os.environ.get("SYNCGUARD_SUBSCRIBER_DSN", "")


def get_validation_cmd() -> str:
    """Command to run full validation (e.g. python main.py)."""
    if hasattr(st, "secrets") and st.secrets.get("validation_command"):
        return st.secrets["validation_command"]
    return os.environ.get("SYNCGUARD_VALIDATION_CMD", "python main.py")


# -----------------------------------------------------------------------------
# Control DB: context manager (no hanging connections)
# -----------------------------------------------------------------------------


@contextmanager
def control_db_connection():
    """Yield (psycopg2_conn, sqlalchemy_engine) for Control DB. Use engine for pd.read_sql to avoid pandas warnings."""
    dsn = get_control_dsn()
    if not dsn:
        yield None, None
        return
    conn = None
    engine = None
    try:
        from sqlalchemy import create_engine
        engine = create_engine(dsn)
        conn = psycopg2.connect(dsn)
        yield conn, engine
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass


def get_subscriber_conn():
    """Subscriber connection for repair execution (caller must close)."""
    dsn = get_subscriber_dsn()
    if not dsn:
        return None
    try:
        return psycopg2.connect(dsn)
    except Exception as e:
        st.error(f"Subscriber DB connection failed: {e}")
        return None


# -----------------------------------------------------------------------------
# Queries
# -----------------------------------------------------------------------------


def fetch_monitored_tables(conn) -> list[str]:
    """Distinct table_name from validation_runs."""
    if conn is None:
        return []
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT table_name FROM {SCHEMA}.validation_runs ORDER BY table_name"
        )
        return [r[0] for r in cur.fetchall()]


def fetch_runs(engine, table_filter: str | None = None, run_id_filter: str | None = None, limit: int | None = None) -> pd.DataFrame:
    """Validation runs as DataFrame. Uses SQLAlchemy engine for pd.read_sql (named params)."""
    if engine is None:
        return pd.DataFrame()
    sql = f"""
    SELECT run_id, table_name, status, started_at, finished_at, mismatch_count
    FROM {SCHEMA}.validation_runs
    """
    conditions = []
    params = {}
    if table_filter:
        conditions.append("table_name = :table_filter")
        params["table_filter"] = table_filter
    if run_id_filter:
        conditions.append("run_id = :run_id_filter")
        params["run_id_filter"] = run_id_filter
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY started_at DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    if params:
        return pd.read_sql(text(sql), engine, params=params)
    return pd.read_sql(text(sql), engine)


def fetch_divergences(engine, run_id: str) -> pd.DataFrame:
    """Divergence log entries for a run. Uses SQLAlchemy engine (named params). resolved_at / is_resolved if columns exist."""
    if engine is None:
        return pd.DataFrame()
    params = {"run_id": run_id}
    sql_full = f"""
    SELECT log_id, run_id, pk_value, publisher_data, subscriber_data, repair_sql, repaired_at, resolved_at
    FROM {SCHEMA}.divergence_log
    WHERE run_id = :run_id
    ORDER BY log_id
    """
    try:
        df = pd.read_sql(text(sql_full), engine, params=params)
        df["is_resolved"] = df["resolved_at"].notna()
        return df
    except Exception:
        sql = f"""
        SELECT log_id, run_id, pk_value, publisher_data, subscriber_data, repair_sql, repaired_at
        FROM {SCHEMA}.divergence_log
        WHERE run_id = :run_id
        ORDER BY log_id
        """
        df = pd.read_sql(text(sql), engine, params=params)
        df["resolved_at"] = pd.NA
        df["is_resolved"] = False
        return df


def fetch_metrics(conn, table_filter: str | None = None) -> dict:
    """Total checks, active divergences, successful repairs."""
    if conn is None:
        return {"total_checks": 0, "active_divergences": 0, "successful_repairs": 0}
    with conn.cursor() as cur:
        sql_total = f"SELECT COUNT(*) FROM {SCHEMA}.validation_runs"
        params = [table_filter] if table_filter else []
        if table_filter:
            sql_total += " WHERE table_name = %s"
        cur.execute(sql_total, params if params else None)
        total_checks = cur.fetchone()[0]

        try:
            sql_active = f"""
            SELECT COUNT(*) FROM {SCHEMA}.divergence_log d
            JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
            WHERE (d.resolved_at IS NULL)
            """
            if table_filter:
                sql_active += " AND r.table_name = %s"
            cur.execute(sql_active, params if params else None)
            active_divergences = cur.fetchone()[0]
        except Exception:
            conn.rollback()
            sql_active = f"""
            SELECT COUNT(*) FROM {SCHEMA}.divergence_log d
            JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
            """
            if table_filter:
                sql_active += " WHERE r.table_name = %s"
            cur.execute(sql_active, params if params else None)
            active_divergences = cur.fetchone()[0]

        sql_repairs = f"""
        SELECT COUNT(*) FROM {SCHEMA}.divergence_log d
        JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
        WHERE d.repaired_at IS NOT NULL
        """
        if table_filter:
            sql_repairs += " AND r.table_name = %s"
        cur.execute(sql_repairs, params if params else None)
        successful_repairs = cur.fetchone()[0]

    return {
        "total_checks": total_checks,
        "active_divergences": active_divergences,
        "successful_repairs": successful_repairs,
    }


def fetch_health_meter(conn) -> dict:
    """Overall Sync % and Pending Repairs for Health Meter. Tries resolved_at then is_resolved; rolls back on error so fallbacks run in a clean transaction."""
    if conn is None:
        return {"total_tables": 0, "diverged_tables": 0, "sync_pct": 100.0, "pending_repairs": 0}
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(DISTINCT table_name) FROM {SCHEMA}.validation_runs")
        total_tables = cur.fetchone()[0] or 0
        try:
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT r.table_name
                    FROM {SCHEMA}.divergence_log d
                    JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
                    WHERE d.resolved_at IS NULL
                ) t
            """)
            diverged_tables = cur.fetchone()[0] or 0
        except Exception:
            conn.rollback()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.divergence_log")
                diverged_tables = cur.fetchone()[0] or 0
            except Exception:
                conn.rollback()
                diverged_tables = 0
        try:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.divergence_log WHERE resolved_at IS NULL")
            pending_repairs = cur.fetchone()[0] or 0
        except Exception:
            conn.rollback()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.divergence_log WHERE (is_resolved IS NULL OR is_resolved = false)")
                pending_repairs = cur.fetchone()[0] or 0
            except Exception:
                conn.rollback()
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.divergence_log")
                    pending_repairs = cur.fetchone()[0] or 0
                except Exception:
                    conn.rollback()
                    pending_repairs = 0

    if total_tables == 0:
        sync_pct = 100.0
    else:
        sync_pct = max(0.0, 100.0 - (diverged_tables / total_tables * 100.0))

    return {
        "total_tables": total_tables,
        "diverged_tables": diverged_tables,
        "sync_pct": round(sync_pct, 1),
        "pending_repairs": pending_repairs,
    }


def parse_insert_columns(repair_sql: str) -> list[str]:
    """Extract column names from INSERT INTO ... ( col1, col2 ) for value substitution."""
    match = re.search(r"INSERT\s+INTO\s+[^(]+\(([^)]+)\)", repair_sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    col_str = match.group(1)
    return [c.strip().strip('"') for c in col_str.split(",")]


def execute_repair(subscriber_conn, repair_sql: str, publisher_data: dict) -> None:
    """Execute repair_sql on subscriber with values from publisher_data (column order from SQL)."""
    cols = parse_insert_columns(repair_sql)
    if not cols:
        raise ValueError("Could not parse column list from repair_sql")
    values = [publisher_data.get(c) for c in cols]
    with subscriber_conn.cursor() as cur:
        sql = re.sub(r"\$\d+", "%s", repair_sql)
        cur.execute(sql, values)
    subscriber_conn.commit()


def mark_log_resolved(control_conn, log_id: int) -> None:
    """Set resolved_at = now() and is_resolved = true if column exists."""
    if control_conn is None:
        return
    with control_conn.cursor() as cur:
        try:
            cur.execute(
                f"UPDATE {SCHEMA}.divergence_log SET resolved_at = %s WHERE log_id = %s",
                (datetime.now(timezone.utc), log_id),
            )
        except Exception:
            pass
        try:
            cur.execute(
                f"UPDATE {SCHEMA}.divergence_log SET is_resolved = true WHERE log_id = %s",
                (log_id,),
            )
        except Exception:
            pass
    control_conn.commit()


# -----------------------------------------------------------------------------
# Subprocess: launch validation
# -----------------------------------------------------------------------------


def is_process_running(pid: int) -> bool:
    """True if process with given PID is still running."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def launch_validation_process(cmd: str):
    """Start validation as background subprocess; return process or None on failure."""
    try:
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return proc
    except Exception as e:
        st.toast(f"Failed to start validation: {e}", icon="❌")
        return None


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------


def main():
    st.set_page_config(page_title="SyncGuard Dashboard", layout="wide")
    st.title("SyncGuard Dashboard — Control Center")

    # Session state for subprocess PID (prevent multiple simultaneous runs)
    if "validation_pid" not in st.session_state:
        st.session_state.validation_pid = None
    if "validation_finished_toast_shown" not in st.session_state:
        st.session_state.validation_finished_toast_shown = False

    # Detect finished background job and toast once
    pid = st.session_state.validation_pid
    if pid is not None and not is_process_running(pid):
        if not st.session_state.validation_finished_toast_shown:
            st.toast("Background validation finished.", icon="✅")
            st.session_state.validation_finished_toast_shown = True
        st.session_state.validation_pid = None

    with control_db_connection() as (conn, engine):
        if conn is None:
            st.warning(
                "Set **SYNCGUARD_CONTROL_DSN** or configure `control_database` in `.streamlit/secrets.toml` to connect to the Control database."
            )
            st.stop()

        # Sidebar: monitored tables
        tables = fetch_monitored_tables(conn)
        st.sidebar.header("Monitored tables")
        table_filter = None
        if tables:
            table_filter = st.sidebar.selectbox(
                "Filter by table",
                options=[None, *tables],
                format_func=lambda x: "(all)" if x is None else x,
            )
        else:
            st.sidebar.info("No validation runs yet. Run SyncGuard to populate.")

        # ----- Launch Full Validation (subprocess) -----
        cmd = get_validation_cmd()
        validation_running = pid is not None and is_process_running(pid)
        if st.button(
            "🚀 Launch Full Validation",
            disabled=validation_running,
            type="primary",
            help=f"Runs in background: {cmd}. Only one run at a time.",
        ):
            proc = launch_validation_process(cmd)
            if proc is not None:
                st.session_state.validation_pid = proc.pid
                st.session_state.validation_finished_toast_shown = False
                st.toast("Background validation started.", icon="🚀")
                st.rerun()

        if validation_running:
            st.sidebar.caption(f"Validation running (PID {pid}). Refresh to update.")

        # ----- Health Meter (top) -----
        health = fetch_health_meter(conn)
        st.subheader("Health meter")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Overall Sync %", f"{health['sync_pct']}%")
        with col2:
            st.metric("Pending Repairs", health["pending_repairs"])
        st.caption(f"Based on {health['total_tables']} table(s); {health['diverged_tables']} with unresolved divergences.")

        # ----- Live Execution Log (last 10 runs, optionally filtered by current run_id) -----
        current_run_id = st.session_state.get("current_run_id")
        with st.expander("🛠 Active Process Logs", expanded=False):
            if current_run_id:
                last_runs = fetch_runs(engine, run_id_filter=current_run_id, limit=10)
            else:
                last_runs = fetch_runs(engine, table_filter=table_filter, limit=10)
            if last_runs.empty:
                st.info("No validation runs yet.")
            else:
                display = last_runs.copy()
                display["run_id"] = display["run_id"].astype(str)
                st.dataframe(display, width="stretch", hide_index=True)
            st.caption("Last 10 validation runs (filtered by selected run when one is chosen). Refresh to update.")

        # ----- Metrics (legacy-style) -----
        metrics = fetch_metrics(conn, table_filter)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Total checks", metrics["total_checks"])
        with c2:
            st.metric("Active divergences", metrics["active_divergences"])
        with c3:
            st.metric("Successful repairs", metrics["successful_repairs"])

        # ----- Active (running) validation runs -----
        runs_df = fetch_runs(engine, table_filter)
        running = runs_df[runs_df["status"] == "running"] if not runs_df.empty else pd.DataFrame()
        if not running.empty:
            with st.status("Validation run(s) in progress", expanded=True) as status:
                for _, row in running.iterrows():
                    st.write(f"**{row['table_name']}** — started {row['started_at']}")
                st.caption("Refresh the page to update.")
                status.update(label="Running", state="running")

        # ----- History: validation runs -----
        st.subheader("Validation runs history")
        if runs_df.empty:
            st.info("No validation runs yet.")
        else:
            runs_df_display = runs_df.copy()
            runs_df_display["run_id"] = runs_df_display["run_id"].astype(str)
            st.dataframe(runs_df_display, width="stretch", hide_index=True)

            run_options = runs_df[runs_df["status"].isin(("diverged", "running"))]
            if not run_options.empty:
                run_ids = run_options["run_id"].tolist()
                labels = [
                    f"{r['table_name']} — {r['started_at']} ({r['status']})"
                    for _, r in run_options.iterrows()
                ]
                selected_idx = st.selectbox(
                    "Select a diverged run to see mismatches",
                    range(len(run_ids)),
                    format_func=lambda i: labels[i],
                )
                selected_run_id = run_ids[selected_idx]
                st.session_state.current_run_id = str(selected_run_id)

                div_df = fetch_divergences(engine, str(selected_run_id))
                if div_df.empty:
                    st.info("No divergence log entries for this run.")
                else:
                    st.subheader("Mismatches & Repair Console")
                    display_cols = ["log_id", "pk_value", "repaired_at"]
                    if "resolved_at" in div_df.columns:
                        display_cols.append("resolved_at")
                    if "is_resolved" in div_df.columns:
                        display_cols.append("is_resolved")
                    display_df = div_df[[c for c in display_cols if c in div_df.columns]].copy()
                    display_df["repaired_at"] = pd.to_datetime(display_df.get("repaired_at"), errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
                    if "resolved_at" in display_df.columns:
                        display_df["resolved_at"] = display_df["resolved_at"].apply(
                            lambda x: x.strftime("%Y-%m-%d %H:%M") if hasattr(x, "strftime") and pd.notna(x) else ""
                        )
                    st.dataframe(display_df, width="stretch", hide_index=True)

                    subscriber_dsn = get_subscriber_dsn()
                    if not subscriber_dsn:
                        st.caption("Set SYNCGUARD_SUBSCRIBER_DSN or subscriber_database in secrets to enable Execute Repair.")
                    else:
                        for _, row in div_df.iterrows():
                            log_id = row["log_id"]
                            resolved = (
                                (row.get("resolved_at") is not None and pd.notna(row.get("resolved_at")))
                                or (row.get("is_resolved") is True)
                            )
                            if resolved:
                                st.caption(f"Log id {log_id} (PK {row['pk_value']}) — already resolved.")
                                continue
                            with st.expander(f"Log id {log_id} — PK {row['pk_value']}"):
                                # Manual Review: repair_query in st.code
                                st.markdown("**Manual Review**")
                                st.code(row["repair_sql"] or "", language="sql")
                                st.json(row["publisher_data"] if row["publisher_data"] else {})
                                if row.get("subscriber_data"):
                                    st.caption("Subscriber (before):")
                                    st.json(row["subscriber_data"])

                                if st.button("Execute Repair", key=f"repair_{log_id}"):
                                    try:
                                        sub_conn = get_subscriber_conn()
                                        if not sub_conn:
                                            st.error("Subscriber connection not available.")
                                        else:
                                            pub_data = row["publisher_data"]
                                            if isinstance(pub_data, str):
                                                pub_data = json.loads(pub_data)
                                            execute_repair(sub_conn, row["repair_sql"], pub_data)
                                            sub_conn.close()
                                            mark_log_resolved(conn, log_id)
                                            st.toast(f"Repair executed for log_id {log_id}.", icon="✅")
                                            st.success(f"Repair executed for log_id {log_id}.")
                                            st.rerun()
                                    except Exception as e:
                                        st.exception(e)
                                        st.toast(f"Repair failed: {e}", icon="❌")

        if not running.empty and st.button("Refresh"):
            st.rerun()


if __name__ == "__main__":
    main()
