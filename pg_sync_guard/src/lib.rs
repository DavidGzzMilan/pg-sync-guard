//! SyncGuard: Background worker for bucket-based hash validation of replicated tables.
//!
//! `CREATE EXTENSION pg_sync_guard` creates the required `syncguard` schema objects:
//!   - `syncguard.monitored_tables`
//!   - `syncguard.hash_catalog`

use std::ffi::{CStr, CString};
use std::time::Duration;

use pgrx::datum::DatumWithOid;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
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

    CREATE TABLE IF NOT EXISTS syncguard.hash_catalog (
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
    DO $$
    BEGIN
        -- Persist the target database name so the static background worker
        -- knows which database to connect to after restart.
        EXECUTE format(
            'ALTER DATABASE %I SET syncguard.database_name = %L',
            current_database(),
            current_database()
        );

        -- Also set it for the current session immediately after CREATE EXTENSION.
        PERFORM set_config('syncguard.database_name', current_database(), false);
    END
    $$;
    "#,
    name = "syncguard_set_database_name",
    finalize
);

// ---------------------------------------------------------------------------
// GUCs
// ---------------------------------------------------------------------------

static SYNCGUARD_DATABASE_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

static SYNCGUARD_NAPTIME: GucSetting<i32> = GucSetting::<i32>::new(60);
static SYNCGUARD_NAPTIME_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static SYNCGUARD_CHUNK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(5000);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    let name = CStr::from_bytes_with_nul(b"syncguard.database_name\0").unwrap();
    GucRegistry::define_string_guc(
        name,
        CStr::from_bytes_with_nul(b"Database to connect for SyncGuard worker\0").unwrap(),
        CStr::from_bytes_with_nul(b"Must be set for the background worker to run.\0").unwrap(),
        &SYNCGUARD_DATABASE_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );

    let name = CStr::from_bytes_with_nul(b"syncguard.naptime\0").unwrap();
    GucRegistry::define_int_guc(
        name,
        CStr::from_bytes_with_nul(b"Seconds between wake-ups\0").unwrap(),
        CStr::from_bytes_with_nul(b"Interval in seconds between worker cycles.\0").unwrap(),
        &SYNCGUARD_NAPTIME,
        1,
        86400,
        GucContext::Sighup,
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
        GucContext::Sighup,
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
        GucContext::Sighup,
        GucFlags::default(),
    );

    BackgroundWorkerBuilder::new("SyncGuard Worker")
        .set_function("syncguard_worker_main")
        .set_library("pg_sync_guard")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .load();
}

#[pg_guard]
pub extern "C-unwind" fn syncguard_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let dbname = match SYNCGUARD_DATABASE_NAME.get().as_ref().and_then(|c| c.to_str().ok()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            pgrx::warning!(
                "SyncGuard Worker: syncguard.database_name not set; idle"
            );
            while BackgroundWorker::wait_latch(Some(Duration::from_secs(60))) {}
            return;
        }
    };

    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

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
    // Get next monitored table (oldest last_checked_at or any without a catalog row)
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
