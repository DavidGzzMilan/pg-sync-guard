//! SyncGuard: dynamic per-database background worker for bucket-based hash
//! validation of monitored tables.
//!
//! `CREATE EXTENSION pg_sync_guard` creates the required `syncguard` schema
//! objects. Start a worker for the current database with:
//! `SELECT syncguard_start_worker();`

use std::ffi::CStr;
use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::datum::DatumWithOid;
use pgrx::spi::{quote_identifier, quote_qualified_identifier};

pgrx::pg_module_magic!();

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS syncguard;

    CREATE TABLE IF NOT EXISTS syncguard.monitored_tables (
        schema_name TEXT NOT NULL,
        table_name  TEXT NOT NULL,
        pk_column   TEXT NOT NULL,
        PRIMARY KEY (schema_name, table_name)
    );

    -- Runtime scan state is reconstructable, so keep it UNLOGGED to avoid
    -- generating WAL for every chunk update.
    CREATE UNLOGGED TABLE IF NOT EXISTS syncguard.hash_catalog (
        schema_name      TEXT NOT NULL,
        table_name       TEXT NOT NULL,
        last_pk_checked  BIGINT NOT NULL DEFAULT 0,
        last_chunk_hash  TEXT,
        last_checked_at  TIMESTAMPTZ,
        PRIMARY KEY (schema_name, table_name)
    );
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

// ---------------------------------------------------------------------------
// GUCs
// ---------------------------------------------------------------------------

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
        CStr::from_bytes_with_nul(b"PK range size per chunk\0").unwrap(),
        CStr::from_bytes_with_nul(b"Number of rows (by PK range) per hash chunk. Default 5000.\0").unwrap(),
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
            Ok(pid) => format!("Started SyncGuard Worker for database '{}' with pid {}", dbname, pid),
            Err(status) => format!(
                "SyncGuard Worker registration succeeded for database '{}' but startup status was {:?}",
                dbname, status
            ),
        },
        Err(_) => format!("Failed to register SyncGuard Worker for database '{}'", dbname),
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
    let chunk_size = SYNCGUARD_CHUNK_SIZE.get().max(1) as i64;

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(naptime_secs as u64))) {
        if BackgroundWorker::sigterm_received() {
            pgrx::log!(
                "SyncGuard Worker: SIGTERM received, shutting down"
            );
            break;
        }

        let result = BackgroundWorker::transaction(|| process_one_chunk(chunk_size));

        if let Err(e) = result {
            pgrx::warning!(
                "SyncGuard Worker: chunk processing error: {}",
                e
            );
        }

        unsafe {
            pg_sys::pg_usleep((naptime_ms * 1000) as core::ffi::c_long);
        }
    }
}

/// Select next table (oldest last_checked_at), get last_pk_checked, run hash chunk, update catalog.
fn process_one_chunk(chunk_size: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get next monitored table (oldest last_checked_at or any without a catalog row).
    // If hash_catalog was truncated after crash recovery (UNLOGGED table), the LEFT JOIN
    // + COALESCE(last_pk_checked, 0) naturally restarts the sweep from PK 0.
    let get_table_sql = "
        SELECT m.schema_name, m.table_name, m.pk_column,
               COALESCE(h.last_pk_checked, 0)::bigint AS last_pk
        FROM syncguard.monitored_tables m
        LEFT JOIN syncguard.hash_catalog h
          ON h.schema_name = m.schema_name AND h.table_name = m.table_name
        ORDER BY h.last_checked_at NULLS FIRST
        LIMIT 1
    ";

    let (schema_name, table_name, pk_column, last_pk): (Option<String>, Option<String>, Option<String>, Option<i64>) =
        Spi::connect_mut(|client| -> spi::Result<(Option<String>, Option<String>, Option<String>, Option<i64>)> {
            let table = client.update(get_table_sql, Some(1), &[])?;
            if table.is_empty() {
                return Ok((None, None, None, None));
            }
            let row = table.first();
            Ok((
                row.get::<String>(1).ok().flatten(),
                row.get::<String>(2).ok().flatten(),
                row.get::<String>(3).ok().flatten(),
                row.get::<i64>(4).ok().flatten(),
            ))
        })?;

    let schema_name = match schema_name {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(()), // no monitored tables
    };
    let table_name = match table_name {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(()),
    };
    let pk_column = match pk_column {
        Some(s) if !s.is_empty() => s,
        _ => {
            pgrx::warning!(
                "SyncGuard Worker: monitored_tables row has empty pk_column for {}.{}",
                schema_name,
                table_name
            );
            return Ok(());
        }
    };
    let start_pk = last_pk.unwrap_or(0);
    let end_pk = start_pk.saturating_add(chunk_size);

    let qualified_table = quote_qualified_identifier(&schema_name, &table_name);
    let quoted_pk = quote_identifier(&pk_column);

    // Build hash query with quoted identifiers only (no user data in SQL structure)
    let hash_sql = format!(
        "WITH b AS (
            SELECT md5(row_to_json(t)::text) AS h
            FROM {} t
            WHERE t.{} >= $1 AND t.{} < $2
            ORDER BY t.{}
        )
        SELECT count(*)::bigint, md5(string_agg(h, '' ORDER BY h)) FROM b",
        qualified_table, quoted_pk, quoted_pk, quoted_pk
    );

    let args: [DatumWithOid<'_>; 2] = [
        DatumWithOid::from(start_pk),
        DatumWithOid::from(end_pk),
    ];

    let (cnt, chunk_hash): (Option<i64>, Option<String>) = match Spi::connect_mut(|client| {
        client
            .update(&hash_sql, Some(1), &args)
            .map(|t| {
                let row = t.first();
                (row.get::<i64>(1).ok().flatten(), row.get::<String>(2).ok().flatten())
            })
    }) {
        Ok(r) => r,
        Err(e) => {
            pgrx::warning!(
                "SyncGuard Worker: table {}.{} hash query failed (table may not exist): {}",
                schema_name,
                table_name,
                e
            );
            return Ok(());
        }
    };

    let cnt = cnt.unwrap_or(0);
    let new_last_pk = if cnt == 0 { 0i64 } else { end_pk };
    let chunk_hash = chunk_hash.unwrap_or_else(|| "".to_string());

    let upsert_sql = "
        INSERT INTO syncguard.hash_catalog (schema_name, table_name, last_pk_checked, last_chunk_hash, last_checked_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (schema_name, table_name)
        DO UPDATE SET
            last_pk_checked = EXCLUDED.last_pk_checked,
            last_chunk_hash = EXCLUDED.last_chunk_hash,
            last_checked_at = EXCLUDED.last_checked_at
    ";

    let upsert_args: [DatumWithOid<'_>; 4] = [
        DatumWithOid::from(schema_name.as_str()),
        DatumWithOid::from(table_name.as_str()),
        DatumWithOid::from(new_last_pk),
        DatumWithOid::from(chunk_hash.as_str()),
    ];

    Spi::run_with_args(upsert_sql, &upsert_args)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_quote_identifiers() {
        let q = pgrx::spi::quote_identifier("my_col");
        assert!(q.contains("my_col"));
        let qq = pgrx::spi::quote_qualified_identifier("public", "my_table");
        assert!(qq.contains("my_table"));
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
