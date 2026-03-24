//! SyncGuard: dynamic per-database background worker that maintains stable
//! per-bucket hashes for monitored tables.
//!
//! The extension is intended to run on both publisher and subscriber.  Each
//! side computes and persists its own bucket hashes locally.  An external CLI
//! can then fetch and compare those bucket hashes across both sides.

use std::error::Error;
use std::ffi::CStr;
use std::time::Duration;

use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::datum::DatumWithOid;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::spi::{quote_identifier, quote_qualified_identifier};

pgrx::pg_module_magic!();

type DynError = Box<dyn Error + Send + Sync>;

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS syncguard;

    CREATE TABLE IF NOT EXISTS syncguard.monitored_tables (
        schema_name TEXT NOT NULL,
        table_name  TEXT NOT NULL,
        pk_column   TEXT NOT NULL,
        bucket_size BIGINT,
        PRIMARY KEY (schema_name, table_name)
    );

    -- Stable per-bucket state. The CLI compares this across publisher/subscriber.
    CREATE UNLOGGED TABLE IF NOT EXISTS syncguard.bucket_catalog (
        schema_name       TEXT NOT NULL,
        table_name        TEXT NOT NULL,
        bucket_id         BIGINT NOT NULL,
        pk_start          BIGINT NOT NULL,
        pk_end            BIGINT NOT NULL,
        row_count         BIGINT NOT NULL DEFAULT 0,
        bucket_hash       TEXT,
        last_computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        dirty             BOOLEAN NOT NULL DEFAULT true,
        reviewed_at       TIMESTAMPTZ,
        PRIMARY KEY (schema_name, table_name, bucket_id)
    );

    CREATE INDEX IF NOT EXISTS syncguard_bucket_catalog_dirty_idx
        ON syncguard.bucket_catalog (schema_name, table_name, dirty);

    -- Queue of buckets that changed and need recomputation.
    CREATE UNLOGGED TABLE IF NOT EXISTS syncguard.dirty_buckets (
        schema_name  TEXT NOT NULL,
        table_name   TEXT NOT NULL,
        bucket_id    BIGINT NOT NULL,
        pk_start     BIGINT NOT NULL,
        pk_end       BIGINT NOT NULL,
        queued_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (schema_name, table_name, bucket_id)
    );

    CREATE INDEX IF NOT EXISTS syncguard_dirty_buckets_queued_idx
        ON syncguard.dirty_buckets (queued_at);

    -- Worker-local progress for the initial full sweep and a stats fallback.
    CREATE UNLOGGED TABLE IF NOT EXISTS syncguard.worker_state (
        schema_name             TEXT NOT NULL,
        table_name              TEXT NOT NULL,
        next_backfill_pk        BIGINT NOT NULL DEFAULT 0,
        initial_sweep_complete  BOOLEAN NOT NULL DEFAULT false,
        last_backfill_at        TIMESTAMPTZ,
        last_stats_total        BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (schema_name, table_name)
    );

    CREATE OR REPLACE FUNCTION syncguard.enqueue_dirty_bucket(
        p_schema_name TEXT,
        p_table_name  TEXT,
        p_pk_value    BIGINT,
        p_bucket_size BIGINT
    ) RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog, syncguard
    AS $$
    DECLARE
        v_bucket_id BIGINT;
        v_pk_start  BIGINT;
        v_pk_end    BIGINT;
    BEGIN
        IF p_pk_value IS NULL OR p_bucket_size IS NULL OR p_bucket_size <= 0 THEN
            RETURN;
        END IF;

        v_bucket_id := p_pk_value / p_bucket_size;
        v_pk_start := v_bucket_id * p_bucket_size;
        v_pk_end := v_pk_start + p_bucket_size;

        INSERT INTO syncguard.dirty_buckets (schema_name, table_name, bucket_id, pk_start, pk_end, queued_at)
        VALUES (p_schema_name, p_table_name, v_bucket_id, v_pk_start, v_pk_end, now())
        ON CONFLICT (schema_name, table_name, bucket_id)
        DO UPDATE SET
            pk_start = EXCLUDED.pk_start,
            pk_end = EXCLUDED.pk_end,
            queued_at = EXCLUDED.queued_at;

        UPDATE syncguard.bucket_catalog
        SET dirty = true,
            reviewed_at = NULL
        WHERE schema_name = p_schema_name
          AND table_name = p_table_name
          AND bucket_id = v_bucket_id;
    END
    $$;

    CREATE OR REPLACE FUNCTION syncguard.mark_dirty_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog, syncguard
    AS $$
    DECLARE
        v_pk_column   TEXT;
        v_bucket_size BIGINT;
        v_old_pk      BIGINT;
        v_new_pk      BIGINT;
    BEGIN
        SELECT
            pk_column,
            COALESCE(bucket_size, current_setting('syncguard.chunk_size', true)::bigint)
        INTO v_pk_column, v_bucket_size
        FROM syncguard.monitored_tables
        WHERE schema_name = TG_TABLE_SCHEMA
          AND table_name = TG_TABLE_NAME;

        IF v_pk_column IS NULL OR v_bucket_size IS NULL OR v_bucket_size <= 0 THEN
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END IF;

        IF TG_OP IN ('INSERT', 'UPDATE') THEN
            EXECUTE format('SELECT ($1).%I::bigint', v_pk_column)
            INTO v_new_pk
            USING NEW;

            PERFORM syncguard.enqueue_dirty_bucket(
                TG_TABLE_SCHEMA,
                TG_TABLE_NAME,
                v_new_pk,
                v_bucket_size
            );
        END IF;

        IF TG_OP IN ('DELETE', 'UPDATE') THEN
            EXECUTE format('SELECT ($1).%I::bigint', v_pk_column)
            INTO v_old_pk
            USING OLD;

            IF TG_OP <> 'UPDATE' OR v_old_pk IS DISTINCT FROM v_new_pk THEN
                PERFORM syncguard.enqueue_dirty_bucket(
                    TG_TABLE_SCHEMA,
                    TG_TABLE_NAME,
                    v_old_pk,
                    v_bucket_size
                );
            END IF;
        END IF;

        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END
    $$;
    "#,
    name = "syncguard_schema_objects",
    bootstrap
);

extension_sql!(
    r#"
    SELECT syncguard_start_worker();
    "#,
    name = "syncguard_autostart_worker",
    requires = [syncguard_start_worker],
    finalize
);

static SYNCGUARD_NAPTIME: GucSetting<i32> = GucSetting::<i32>::new(60);
static SYNCGUARD_NAPTIME_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static SYNCGUARD_CHUNK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(5000);

fn current_database_name() -> Option<String> {
    unsafe {
        let dbid = pg_sys::MyDatabaseId;
        if dbid == pg_sys::InvalidOid {
            return None;
        }

        let dbname_ptr = pg_sys::get_database_name(dbid);
        if dbname_ptr.is_null() {
            return None;
        }

        Some(CStr::from_ptr(dbname_ptr).to_string_lossy().into_owned())
    }
}

fn bucket_bounds(pk_value: i64, bucket_size: i64) -> (i64, i64, i64) {
    let bucket_id = pk_value / bucket_size;
    let pk_start = bucket_id * bucket_size;
    let pk_end = pk_start + bucket_size;
    (bucket_id, pk_start, pk_end)
}

fn current_stats_total(schema_name: &str, table_name: &str) -> Result<i64, DynError> {
    let sql = "
        SELECT COALESCE(n_tup_ins + n_tup_upd + n_tup_del, 0)::bigint
        FROM pg_stat_user_tables
        WHERE schemaname = $1
          AND relname = $2
    ";
    let args = [
        DatumWithOid::from(schema_name),
        DatumWithOid::from(table_name),
    ];

    Ok(Spi::get_one_with_args::<i64>(sql, &args)?.unwrap_or(0))
}

fn install_dirty_trigger(schema_name: &str, table_name: &str) -> Result<(), DynError> {
    let qualified_table = quote_qualified_identifier(schema_name, table_name);
    let sql = format!(
        "
        DROP TRIGGER IF EXISTS syncguard_dirty_bucket_trigger ON {qualified_table};
        CREATE TRIGGER syncguard_dirty_bucket_trigger
        AFTER INSERT OR UPDATE OR DELETE ON {qualified_table}
        FOR EACH ROW
        EXECUTE FUNCTION syncguard.mark_dirty_trigger();
        ALTER TABLE {qualified_table}
            ENABLE ALWAYS TRIGGER syncguard_dirty_bucket_trigger;
        "
    );
    Spi::run(&sql)?;
    Ok(())
}

fn recompute_bucket(
    schema_name: &str,
    table_name: &str,
    pk_column: &str,
    bucket_id: i64,
    pk_start: i64,
    pk_end: i64,
) -> Result<(), DynError> {
    let qualified_table = quote_qualified_identifier(schema_name, table_name);
    let quoted_pk = quote_identifier(pk_column);

    let hash_sql = format!(
        "WITH b AS (
            SELECT md5(row_to_json(t)::text) AS h
            FROM {qualified_table} t
            WHERE t.{quoted_pk} >= $1 AND t.{quoted_pk} < $2
            ORDER BY t.{quoted_pk}
        )
        SELECT count(*)::bigint, md5(string_agg(h, '' ORDER BY h))
        FROM b"
    );
    let hash_args = [DatumWithOid::from(pk_start), DatumWithOid::from(pk_end)];
    let (row_count, bucket_hash) = Spi::get_two_with_args::<i64, String>(&hash_sql, &hash_args)?;
    let row_count = row_count.unwrap_or(0);

    if row_count == 0 {
        let delete_sql = "
            DELETE FROM syncguard.bucket_catalog
            WHERE schema_name = $1
              AND table_name = $2
              AND bucket_id = $3
        ";
        let delete_args = [
            DatumWithOid::from(schema_name),
            DatumWithOid::from(table_name),
            DatumWithOid::from(bucket_id),
        ];
        Spi::run_with_args(delete_sql, &delete_args)?;
        return Ok(());
    }

    let upsert_sql = "
        INSERT INTO syncguard.bucket_catalog (
            schema_name,
            table_name,
            bucket_id,
            pk_start,
            pk_end,
            row_count,
            bucket_hash,
            last_computed_at,
            dirty,
            reviewed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, now(), false, NULL)
        ON CONFLICT (schema_name, table_name, bucket_id)
        DO UPDATE SET
            pk_start = EXCLUDED.pk_start,
            pk_end = EXCLUDED.pk_end,
            row_count = EXCLUDED.row_count,
            bucket_hash = EXCLUDED.bucket_hash,
            last_computed_at = EXCLUDED.last_computed_at,
            dirty = false,
            reviewed_at = NULL
    ";
    let upsert_args = [
        DatumWithOid::from(schema_name),
        DatumWithOid::from(table_name),
        DatumWithOid::from(bucket_id),
        DatumWithOid::from(pk_start),
        DatumWithOid::from(pk_end),
        DatumWithOid::from(row_count),
        DatumWithOid::from(bucket_hash.unwrap_or_default()),
    ];
    Spi::run_with_args(upsert_sql, &upsert_args)?;
    Ok(())
}

fn process_next_dirty_bucket() -> Result<bool, DynError> {
    let sql = "
        WITH claimed AS (
            DELETE FROM syncguard.dirty_buckets d
            WHERE d.ctid = (
                SELECT d2.ctid
                FROM syncguard.dirty_buckets d2
                ORDER BY d2.queued_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING
                d.schema_name,
                d.table_name,
                d.bucket_id,
                d.pk_start,
                d.pk_end
        )
        SELECT
            c.schema_name,
            c.table_name,
            m.pk_column,
            c.bucket_id,
            c.pk_start,
            c.pk_end
        FROM claimed c
        JOIN syncguard.monitored_tables m
          ON m.schema_name = c.schema_name
         AND m.table_name = c.table_name
    ";
    let maybe_bucket = Spi::connect_mut(|client| -> spi::Result<_> {
        let table = client.update(sql, Some(1), &[])?;
        if table.is_empty() {
            return Ok(None);
        }
        let row = table.first();
        Ok(Some((
            row.get::<String>(1)?,
            row.get::<String>(2)?,
            row.get::<String>(3)?,
            row.get::<i64>(4)?,
            row.get::<i64>(5)?,
            row.get::<i64>(6)?,
        )))
    })?;

    let Some((
        Some(schema_name),
        Some(table_name),
        Some(pk_column),
        Some(bucket_id),
        Some(pk_start),
        Some(pk_end),
    )) = maybe_bucket
    else {
        return Ok(false);
    };

    recompute_bucket(
        &schema_name,
        &table_name,
        &pk_column,
        bucket_id,
        pk_start,
        pk_end,
    )?;

    Ok(true)
}

fn process_next_backfill_bucket(default_bucket_size: i64) -> Result<bool, DynError> {
    let select_sql = "
        SELECT
            m.schema_name,
            m.table_name,
            m.pk_column,
            COALESCE(m.bucket_size, $1::bigint) AS effective_bucket_size,
            COALESCE(w.next_backfill_pk, 0)::bigint AS next_backfill_pk
        FROM syncguard.monitored_tables m
        LEFT JOIN syncguard.worker_state w
          ON w.schema_name = m.schema_name
         AND w.table_name = m.table_name
        WHERE COALESCE(w.initial_sweep_complete, false) = false
        ORDER BY w.last_backfill_at NULLS FIRST, m.schema_name, m.table_name
        LIMIT 1
    ";
    let select_args = [DatumWithOid::from(default_bucket_size)];
    let maybe_table = Spi::connect_mut(|client| -> spi::Result<_> {
        let table = client.update(select_sql, Some(1), &select_args)?;
        if table.is_empty() {
            return Ok(None);
        }
        let row = table.first();
        Ok(Some((
            row.get::<String>(1)?,
            row.get::<String>(2)?,
            row.get::<String>(3)?,
            row.get::<i64>(4)?,
            row.get::<i64>(5)?,
        )))
    })?;

    let Some((
        Some(schema_name),
        Some(table_name),
        Some(pk_column),
        Some(bucket_size),
        Some(next_backfill_pk),
    )) = maybe_table
    else {
        return Ok(false);
    };

    let qualified_table = quote_qualified_identifier(&schema_name, &table_name);
    let quoted_pk = quote_identifier(&pk_column);
    let min_max_sql = format!(
        "SELECT min(t.{quoted_pk})::bigint, max(t.{quoted_pk})::bigint
         FROM {qualified_table} t"
    );
    let (min_pk, max_pk) = Spi::get_two::<i64, i64>(&min_max_sql)?;

    let worker_update_sql = "
        INSERT INTO syncguard.worker_state (
            schema_name,
            table_name,
            next_backfill_pk,
            initial_sweep_complete,
            last_backfill_at,
            last_stats_total
        )
        VALUES ($1, $2, $3, $4, now(), $5)
        ON CONFLICT (schema_name, table_name)
        DO UPDATE SET
            next_backfill_pk = EXCLUDED.next_backfill_pk,
            initial_sweep_complete = EXCLUDED.initial_sweep_complete,
            last_backfill_at = EXCLUDED.last_backfill_at,
            last_stats_total = EXCLUDED.last_stats_total
    ";

    match (min_pk, max_pk) {
        (Some(min_pk), Some(max_pk)) => {
            let min_bucket_start = (min_pk / bucket_size) * bucket_size;
            let start_pk = if next_backfill_pk < min_bucket_start {
                min_bucket_start
            } else {
                next_backfill_pk
            };

            if start_pk > max_pk {
                let stats_total = current_stats_total(&schema_name, &table_name)?;
                let args = [
                    DatumWithOid::from(schema_name.as_str()),
                    DatumWithOid::from(table_name.as_str()),
                    DatumWithOid::from(0i64),
                    DatumWithOid::from(true),
                    DatumWithOid::from(stats_total),
                ];
                Spi::run_with_args(worker_update_sql, &args)?;
                return Ok(true);
            }

            let (_, pk_start, pk_end) = bucket_bounds(start_pk, bucket_size);
            let bucket_id = pk_start / bucket_size;
            recompute_bucket(
                &schema_name,
                &table_name,
                &pk_column,
                bucket_id,
                pk_start,
                pk_end,
            )?;

            let stats_total = current_stats_total(&schema_name, &table_name)?;
            let args = [
                DatumWithOid::from(schema_name.as_str()),
                DatumWithOid::from(table_name.as_str()),
                DatumWithOid::from(pk_end),
                DatumWithOid::from(false),
                DatumWithOid::from(stats_total),
            ];
            Spi::run_with_args(worker_update_sql, &args)?;
            Ok(true)
        }
        _ => {
            let stats_total = current_stats_total(&schema_name, &table_name)?;
            let args = [
                DatumWithOid::from(schema_name.as_str()),
                DatumWithOid::from(table_name.as_str()),
                DatumWithOid::from(0i64),
                DatumWithOid::from(true),
                DatumWithOid::from(stats_total),
            ];
            Spi::run_with_args(worker_update_sql, &args)?;
            Ok(true)
        }
    }
}

fn process_stats_rescan_fallback() -> Result<bool, DynError> {
    let sql = "
        SELECT
            m.schema_name,
            m.table_name,
            COALESCE(s.n_tup_ins + s.n_tup_upd + s.n_tup_del, 0)::bigint AS current_stats_total,
            COALESCE(w.last_stats_total, 0)::bigint AS last_stats_total
        FROM syncguard.monitored_tables m
        LEFT JOIN syncguard.worker_state w
          ON w.schema_name = m.schema_name
         AND w.table_name = m.table_name
        LEFT JOIN pg_stat_user_tables s
          ON s.schemaname = m.schema_name
         AND s.relname = m.table_name
        WHERE COALESCE(w.initial_sweep_complete, false) = true
          AND COALESCE(s.n_tup_ins + s.n_tup_upd + s.n_tup_del, 0)::bigint > COALESCE(w.last_stats_total, 0)::bigint
        ORDER BY (COALESCE(s.n_tup_ins + s.n_tup_upd + s.n_tup_del, 0)::bigint - COALESCE(w.last_stats_total, 0)::bigint) DESC
        LIMIT 1
    ";

    let maybe_table = Spi::connect_mut(|client| -> spi::Result<_> {
        let table = client.update(sql, Some(1), &[])?;
        if table.is_empty() {
            return Ok(None);
        }
        let row = table.first();
        Ok(Some((
            row.get::<String>(1)?,
            row.get::<String>(2)?,
            row.get::<i64>(3)?,
        )))
    })?;

    let Some((Some(schema_name), Some(table_name), Some(current_total))) = maybe_table else {
        return Ok(false);
    };

    let reset_sql = "
        INSERT INTO syncguard.worker_state (
            schema_name,
            table_name,
            next_backfill_pk,
            initial_sweep_complete,
            last_backfill_at,
            last_stats_total
        )
        VALUES ($1, $2, 0, false, now(), $3)
        ON CONFLICT (schema_name, table_name)
        DO UPDATE SET
            next_backfill_pk = 0,
            initial_sweep_complete = false,
            last_backfill_at = EXCLUDED.last_backfill_at,
            last_stats_total = EXCLUDED.last_stats_total
    ";
    let args = [
        DatumWithOid::from(schema_name.as_str()),
        DatumWithOid::from(table_name.as_str()),
        DatumWithOid::from(current_total),
    ];
    Spi::run_with_args(reset_sql, &args)?;
    Ok(true)
}

fn process_one_task(default_bucket_size: i64) -> Result<(), DynError> {
    if process_next_dirty_bucket()? {
        return Ok(());
    }

    if process_next_backfill_bucket(default_bucket_size)? {
        return Ok(());
    }

    if process_stats_rescan_fallback()? {
        return Ok(());
    }

    Ok(())
}

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    let name = CStr::from_bytes_with_nul(b"syncguard.naptime\0").unwrap();
    GucRegistry::define_int_guc(
        name,
        CStr::from_bytes_with_nul(b"Seconds between wake-ups\0").unwrap(),
        CStr::from_bytes_with_nul(b"Interval in seconds between worker cycles.\0").unwrap(),
        &SYNCGUARD_NAPTIME,
        1,
        86400,
        GucContext::Suset,
        GucFlags::default(),
    );

    let name = CStr::from_bytes_with_nul(b"syncguard.naptime_ms\0").unwrap();
    GucRegistry::define_int_guc(
        name,
        CStr::from_bytes_with_nul(b"Milliseconds to sleep after each chunk\0").unwrap(),
        CStr::from_bytes_with_nul(b"Yields CPU and I/O after processing one chunk.\0").unwrap(),
        &SYNCGUARD_NAPTIME_MS,
        0,
        60000,
        GucContext::Suset,
        GucFlags::default(),
    );

    let name = CStr::from_bytes_with_nul(b"syncguard.chunk_size\0").unwrap();
    GucRegistry::define_int_guc(
        name,
        CStr::from_bytes_with_nul(b"Default PK bucket size\0").unwrap(),
        CStr::from_bytes_with_nul(
            b"Used when monitored_tables.bucket_size is NULL. Default 5000.\0",
        )
        .unwrap(),
        &SYNCGUARD_CHUNK_SIZE,
        100,
        1_000_000,
        GucContext::Suset,
        GucFlags::default(),
    );
}

#[pg_extern]
fn syncguard_worker_running() -> bool {
    let dbname = match current_database_name() {
        Some(dbname) => dbname,
        None => return false,
    };

    let sql = "
        SELECT EXISTS (
            SELECT 1
            FROM pg_stat_activity
            WHERE datname = $1
              AND backend_type = 'SyncGuard Worker'
        )
    ";
    let args = [DatumWithOid::from(dbname.as_str())];

    match Spi::get_one_with_args::<bool>(sql, &args) {
        Ok(Some(is_running)) => is_running,
        _ => false,
    }
}

#[pg_extern]
fn syncguard_start_worker() -> String {
    if syncguard_worker_running() {
        return "SyncGuard Worker already running for current database".to_string();
    }

    let dbname = match current_database_name() {
        Some(dbname) => dbname,
        None => return "Could not determine current database".to_string(),
    };

    let worker_name = format!("SyncGuard Worker [{}]", dbname);
    let handle = BackgroundWorkerBuilder::new(&worker_name)
        .set_type("SyncGuard Worker")
        .set_function("syncguard_worker_main")
        .set_library("pg_sync_guard")
        .set_extra(&dbname)
        .set_notify_pid(unsafe { pg_sys::MyProcPid })
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .load_dynamic();

    match handle {
        Ok(handle) => match handle.wait_for_startup() {
            Ok(pid) => format!(
                "Started SyncGuard Worker for database '{}' with pid {}",
                dbname, pid
            ),
            Err(status) => format!(
                "SyncGuard Worker registration succeeded for database '{}' but startup status was {:?}",
                dbname, status
            ),
        },
        Err(_) => format!(
            "Failed to register SyncGuard Worker for database '{}'",
            dbname
        ),
    }
}

#[pg_extern]
fn syncguard_stop_worker() -> String {
    let dbname = match current_database_name() {
        Some(dbname) => dbname,
        None => return "Could not determine current database".to_string(),
    };

    let sql = "
        SELECT count(*)
        FROM pg_stat_activity
        WHERE datname = $1
          AND backend_type = 'SyncGuard Worker'
          AND pg_terminate_backend(pid)
    ";
    let args = [DatumWithOid::from(dbname.as_str())];

    match Spi::get_one_with_args::<i64>(sql, &args) {
        Ok(Some(terminated)) => format!(
            "Requested shutdown for {} SyncGuard Worker(s) in database '{}'",
            terminated, dbname
        ),
        _ => format!("Could not stop SyncGuard Worker in database '{}'", dbname),
    }
}

#[pg_extern]
fn syncguard_register_table(
    schema_name: &str,
    table_name: &str,
    pk_column: &str,
    bucket_size: default!(Option<i64>, "NULL"),
) -> String {
    let effective_bucket_size = bucket_size.filter(|size| *size > 0);
    let upsert_sql = "
        INSERT INTO syncguard.monitored_tables (schema_name, table_name, pk_column, bucket_size)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (schema_name, table_name)
        DO UPDATE SET
            pk_column = EXCLUDED.pk_column,
            bucket_size = EXCLUDED.bucket_size
    ";
    let upsert_args = [
        DatumWithOid::from(schema_name),
        DatumWithOid::from(table_name),
        DatumWithOid::from(pk_column),
        match effective_bucket_size {
            Some(size) => DatumWithOid::from(size),
            None => DatumWithOid::null::<i64>(),
        },
    ];

    let result = (|| -> Result<String, DynError> {
        Spi::run_with_args(upsert_sql, &upsert_args)?;

        Spi::run_with_args(
            "
            DELETE FROM syncguard.bucket_catalog
            WHERE schema_name = $1
              AND table_name = $2
            ",
            &[
                DatumWithOid::from(schema_name),
                DatumWithOid::from(table_name),
            ],
        )?;
        Spi::run_with_args(
            "
            DELETE FROM syncguard.dirty_buckets
            WHERE schema_name = $1
              AND table_name = $2
            ",
            &[
                DatumWithOid::from(schema_name),
                DatumWithOid::from(table_name),
            ],
        )?;

        let stats_total = current_stats_total(schema_name, table_name)?;
        Spi::run_with_args(
            "
            INSERT INTO syncguard.worker_state (
                schema_name,
                table_name,
                next_backfill_pk,
                initial_sweep_complete,
                last_backfill_at,
                last_stats_total
            )
            VALUES ($1, $2, 0, false, NULL, $3)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET
                next_backfill_pk = 0,
                initial_sweep_complete = false,
                last_backfill_at = NULL,
                last_stats_total = EXCLUDED.last_stats_total
            ",
            &[
                DatumWithOid::from(schema_name),
                DatumWithOid::from(table_name),
                DatumWithOid::from(stats_total),
            ],
        )?;

        install_dirty_trigger(schema_name, table_name)?;

        Ok(format!(
            "Registered {}.{} (pk={}, bucket_size={})",
            schema_name,
            table_name,
            pk_column,
            effective_bucket_size.unwrap_or(SYNCGUARD_CHUNK_SIZE.get().max(1) as i64)
        ))
    })();

    match result {
        Ok(msg) => msg,
        Err(e) => format!("Failed to register {}.{}: {}", schema_name, table_name, e),
    }
}

#[pg_extern]
fn syncguard_mark_bucket_reviewed(schema_name: &str, table_name: &str, bucket_id: i64) -> bool {
    let sql = "
        UPDATE syncguard.bucket_catalog
        SET reviewed_at = now()
        WHERE schema_name = $1
          AND table_name = $2
          AND bucket_id = $3
    ";
    let args = [
        DatumWithOid::from(schema_name),
        DatumWithOid::from(table_name),
        DatumWithOid::from(bucket_id),
    ];
    Spi::run_with_args(sql, &args).is_ok()
}

#[unsafe(no_mangle)]
#[pg_guard]
pub extern "C-unwind" fn syncguard_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let dbname = BackgroundWorker::get_extra();
    if dbname.is_empty() {
        pgrx::warning!("SyncGuard Worker: no target database provided");
        return;
    }

    BackgroundWorker::connect_worker_to_spi(Some(dbname), None);

    let naptime_secs = SYNCGUARD_NAPTIME.get().max(1);
    let naptime_ms = SYNCGUARD_NAPTIME_MS.get().max(0) as u64;
    let default_bucket_size = SYNCGUARD_CHUNK_SIZE.get().max(1) as i64;

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(naptime_secs as u64))) {
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("SyncGuard Worker: SIGTERM received, shutting down");
            break;
        }

        let result = BackgroundWorker::transaction(|| process_one_task(default_bucket_size));
        if let Err(e) = result {
            pgrx::warning!("SyncGuard Worker: task processing error: {}", e);
        }

        unsafe {
            pg_sys::pg_usleep((naptime_ms * 1000) as core::ffi::c_long);
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_bucket_bounds() {
        let (bucket_id, pk_start, pk_end) = crate::bucket_bounds(42, 10);
        assert_eq!(bucket_id, 4);
        assert_eq!(pk_start, 40);
        assert_eq!(pk_end, 50);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
