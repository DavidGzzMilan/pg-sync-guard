"""
SyncGuard Streamlit Dashboard

Connects to the Control PostgreSQL database to read syncguard.validation_runs
and syncguard.divergence_log. Optional subscriber connection to execute repairs
from the UI.
"""

import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st

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


@st.cache_resource(ttl=60)
def get_control_conn():
    """Cached control DB connection."""
    dsn = get_control_dsn()
    if not dsn:
        return None
    try:
        return psycopg2.connect(dsn)
    except Exception as e:
        st.error(f"Control DB connection failed: {e}")
        return None


def get_subscriber_conn():
    """Subscriber connection (not cached; used only when repairing)."""
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
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT DISTINCT table_name FROM {SCHEMA}.validation_runs ORDER BY table_name'
        )
        return [r[0] for r in cur.fetchall()]


def fetch_runs(conn, table_filter: str | None = None) -> pd.DataFrame:
    """Validation runs as DataFrame."""
    sql = f"""
    SELECT run_id, table_name, status, started_at, finished_at, mismatch_count
    FROM {SCHEMA}.validation_runs
    """
    if table_filter:
        sql += " WHERE table_name = %s"
        sql += " ORDER BY started_at DESC"
        return pd.read_sql(sql, conn, params=[table_filter])
    sql += " ORDER BY started_at DESC"
    return pd.read_sql(sql, conn)


def fetch_divergences(conn, run_id: str) -> pd.DataFrame:
    """Divergence log entries for a run. resolved_at included if column exists."""
    sql_full = f"""
    SELECT log_id, run_id, pk_value, publisher_data, subscriber_data, repair_sql, repaired_at, resolved_at
    FROM {SCHEMA}.divergence_log
    WHERE run_id = %s
    ORDER BY log_id
    """
    try:
        return pd.read_sql(sql_full, conn, params=(run_id,))
    except Exception:
        sql = f"""
        SELECT log_id, run_id, pk_value, publisher_data, subscriber_data, repair_sql, repaired_at
        FROM {SCHEMA}.divergence_log
        WHERE run_id = %s
        ORDER BY log_id
        """
        df = pd.read_sql(sql, conn, params=(run_id,))
        df["resolved_at"] = pd.NA
        return df


def fetch_metrics(conn, table_filter: str | None = None) -> dict:
    """Total checks, active divergences (unresolved), successful repairs."""
    with conn.cursor() as cur:
        # Total validation runs (checks)
        sql_total = f"SELECT COUNT(*) FROM {SCHEMA}.validation_runs"
        params = []
        if table_filter:
            sql_total += " WHERE table_name = %s"
            params.append(table_filter)
        cur.execute(sql_total, params if params else None)
        total_checks = cur.fetchone()[0]

        # Active divergences: divergence_log rows not yet resolved (resolved_at IS NULL)
        try:
            sql_active = f"""
            SELECT COUNT(*) FROM {SCHEMA}.divergence_log d
            JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
            WHERE d.resolved_at IS NULL
            """
            params_active = [table_filter] if table_filter else []
            if table_filter:
                sql_active += " AND r.table_name = %s"
            cur.execute(sql_active, params_active if params_active else None)
            active_divergences = cur.fetchone()[0]
        except Exception:
            # resolved_at column may not exist yet; count all as active
            sql_active = f"""
            SELECT COUNT(*) FROM {SCHEMA}.divergence_log d
            JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
            """
            params_active = [table_filter] if table_filter else []
            if table_filter:
                sql_active += " WHERE r.table_name = %s"
            cur.execute(sql_active, params_active if params_active else None)
            active_divergences = cur.fetchone()[0]

        # Successful repairs: rows that have repaired_at (were applied by SyncGuard)
        sql_repairs = f"""
        SELECT COUNT(*) FROM {SCHEMA}.divergence_log d
        JOIN {SCHEMA}.validation_runs r ON r.run_id = d.run_id
        WHERE d.repaired_at IS NOT NULL
        """
        params_repairs = [table_filter] if table_filter else []
        if table_filter:
            sql_repairs += " AND r.table_name = %s"
        cur.execute(sql_repairs, params_repairs if params_repairs else None)
        successful_repairs = cur.fetchone()[0]
    return {
        "total_checks": total_checks,
        "active_divergences": active_divergences,
        "successful_repairs": successful_repairs,
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
        # Use %s placeholders for psycopg2 (repair_sql uses $1,$2 from asyncpg; we need to replace)
        sql = re.sub(r"\$\d+", "%s", repair_sql)
        cur.execute(sql, values)
    subscriber_conn.commit()


def mark_log_resolved(control_conn, log_id: int) -> None:
    """Set resolved_at = now() for the given log_id."""
    with control_conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.divergence_log SET resolved_at = %s WHERE log_id = %s",
            (datetime.now(timezone.utc), log_id),
        )
    control_conn.commit()


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------


def main():
    st.set_page_config(page_title="SyncGuard Dashboard", layout="wide")
    st.title("SyncGuard Dashboard")

    conn = get_control_conn()
    if not conn:
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

    # Metrics
    metrics = fetch_metrics(conn, table_filter)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total checks", metrics["total_checks"])
    with col2:
        st.metric("Active divergences", metrics["active_divergences"])
    with col3:
        st.metric("Successful repairs", metrics["successful_repairs"])

    # Active (running) validation runs
    runs_df = fetch_runs(conn, table_filter)
    running = runs_df[runs_df["status"] == "running"] if not runs_df.empty else pd.DataFrame()
    if not running.empty:
        with st.status("Validation run(s) in progress", expanded=True) as status:
            for _, row in running.iterrows():
                st.write(f"**{row['table_name']}** — started {row['started_at']}")
            st.caption("Refresh the page to update.")
            status.update(label="Running", state="running")

    # History: validation runs dataframe
    st.subheader("Validation runs history")
    if runs_df.empty:
        st.info("No validation runs yet.")
    else:
        runs_df_display = runs_df.copy()
        runs_df_display["run_id"] = runs_df_display["run_id"].astype(str)
        st.dataframe(runs_df_display, use_container_width=True, hide_index=True)

        # Select a run to see divergences
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

            div_df = fetch_divergences(conn, str(selected_run_id))
            if div_df.empty:
                st.info("No divergence log entries for this run.")
            else:
                st.subheader("Mismatches")
                # Show table (without raw repair_sql in main view for readability)
                display_cols = ["log_id", "pk_value", "repaired_at"]
                if "resolved_at" in div_df.columns:
                    display_cols.append("resolved_at")
                display_df = div_df[display_cols].copy()
                display_df["repaired_at"] = pd.to_datetime(display_df["repaired_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
                if "resolved_at" in display_df.columns:
                    display_df["resolved_at"] = display_df["resolved_at"].apply(
                        lambda x: x.strftime("%Y-%m-%d %H:%M") if hasattr(x, "strftime") and pd.notna(x) else ""
                    )
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                # Repair buttons per row
                subscriber_dsn = get_subscriber_dsn()
                if not subscriber_dsn:
                    st.caption("Set SYNCGUARD_SUBSCRIBER_DSN or subscriber_database in secrets to enable Repair.")
                else:
                    for _, row in div_df.iterrows():
                        log_id = row["log_id"]
                        resolved = (
                            "resolved_at" in row
                            and row.get("resolved_at") is not None
                            and pd.notna(row["resolved_at"])
                        )
                        if resolved:
                            st.caption(f"Log id {log_id} (PK {row['pk_value']}) — already resolved.")
                            continue
                        with st.expander(f"Log id {log_id} — PK {row['pk_value']}"):
                            st.json(row["publisher_data"] if row["publisher_data"] else {})
                            if row["subscriber_data"]:
                                st.caption("Subscriber (before):")
                                st.json(row["subscriber_data"])
                            st.code(row["repair_sql"] or "", language="sql")
                            if st.button("Repair", key=f"repair_{log_id}"):
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
                                        try:
                                            mark_log_resolved(conn, log_id)
                                        except Exception:
                                            st.warning("Repair applied; add column `resolved_at` to syncguard.divergence_log to mark as resolved.")
                                        st.success(f"Repair executed for log_id {log_id}.")
                                        st.rerun()
                                except Exception as e:
                                    st.exception(e)

    # Auto-refresh option for active runs
    if not running.empty:
        if st.button("Refresh"):
            st.rerun()


if __name__ == "__main__":
    main()
