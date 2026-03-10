# Control Plane (optional)

The `pg_sync_guard` extension keeps **current local hash state** inside each database.

An optional **control plane** is a separate database used by the external CLI to persist:

- verification job history
- bucket-level divergence findings
- review / remediation workflow state

This keeps the extension lean while still allowing operational history and auditability.

## Suggested schema

Create this in a dedicated control database, or in another schema/database used for SyncGuard operations:

```sql
CREATE SCHEMA IF NOT EXISTS syncguard;

CREATE TABLE syncguard.validation_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running', -- running, success, diverged, failed
    publisher_name TEXT NOT NULL,
    subscriber_name TEXT NOT NULL,
    table_filter TEXT,
    total_buckets_compared BIGINT NOT NULL DEFAULT 0,
    mismatched_buckets BIGINT NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE TABLE syncguard.divergence_log (
    log_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES syncguard.validation_runs(run_id),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    bucket_id BIGINT NOT NULL,
    pk_start BIGINT NOT NULL,
    pk_end BIGINT NOT NULL,
    publisher_row_count BIGINT,
    subscriber_row_count BIGINT,
    publisher_hash TEXT,
    subscriber_hash TEXT,
    status TEXT NOT NULL DEFAULT 'open', -- open, reviewed, resolved, ignored
    review_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ
);
```

## How it fits the architecture

The intended split is:

- extension:
  - computes and stores current bucket state locally
  - exposes `syncguard.bucket_catalog`, `syncguard.dirty_buckets`, and worker helpers
- external CLI:
  - connects to publisher and subscriber
  - fetches bucket hashes from both sides
  - compares them
  - writes verification results into the control plane

## Typical CLI flow

1. Insert a row into `syncguard.validation_runs` with `status = 'running'`
2. Read bucket hashes from publisher and subscriber
3. Compare buckets by `(schema_name, table_name, bucket_id)`
4. Insert mismatches into `syncguard.divergence_log`
5. Update the run with:
   - `finished_at`
   - `status`
   - `total_buckets_compared`
   - `mismatched_buckets`

## Notes

- The control plane is intentionally separate from the extension state.
- `bucket_catalog` on publisher/subscriber should represent **current state**, not history.
- Historical reporting belongs in the control plane, not inside the extension tables.
