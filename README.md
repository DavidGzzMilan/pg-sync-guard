# pg-sync-guard

Validate data consistency between a PostgreSQL Logical Replication **Publisher** and **Subscriber** using bucket hashing, then pinpoint and repair mismatches.

## Features

- **Schema analysis**: Inspects the target table on the Publisher to discover the Primary Key and high-churn columns (by type).
- **Bucket hash validation**: Splits the table into \(N\) segments by PK order, computes a deterministic per-segment hash with `md5(row_to_json(...))` and `string_agg(..., '' ORDER BY pk)`, runs the same query on both sides in parallel, and compares results.
- **Pinpoint & repair**: On mismatch, recursively narrows the segment (same hash query restricted to the segment’s PK range with \(N=2\)) until the range has at most a small batch of rows; then fetches those rows from the Publisher and runs `INSERT ... ON CONFLICT DO UPDATE` on the Subscriber.

## Install

```bash
pip install -e .
# or
pip install -r requirements.txt
```

## Quick usage

```python
import asyncio
import asyncpg
from sync_guard import SyncGuard, validate_and_repair

async def main():
    pub = await asyncpg.connect("postgres://user:pass@publisher-host/db")
    sub = await asyncpg.connect("postgres://user:pass@subscriber-host/db")
    guard = SyncGuard(pub, sub, num_segments=32)
    repaired = await validate_and_repair(guard, "public", "my_table")
    print("Repaired PKs:", repaired)
    await pub.close()
    await sub.close()

asyncio.run(main())
```

## Class structure

| Component | Role |
|-----------|------|
| **SyncGuard** | Holds publisher/subscriber connections and table metadata; runs parallel hash queries and comparison. |
| **analyze_schema(schema, table)** | Inspects table on Publisher → PK columns and high-churn column list. |
| **validate(schema, table)** | Runs bucket hash on both DBs in parallel; returns list of mismatched segments. |
| **validate_and_repair(guard, schema, table)** | Validates, then for each mismatched segment recursively pinpoints and repairs (upsert from Publisher to Subscriber). |
| **sync_guard.schema** | `analyze_table()`, `TableInfo`, `ColumnInfo`, high-churn type set. |
| **sync_guard.hash_queries** | Builds full-table and bounded bucket-hash SQL, row-count, and upsert SQL. |

## Recursive SQL logic (hashing)

**1. Full-table bucket hash (initial validation)**

- **CTE `base`**: `SELECT * FROM "schema"."table"`.
- **CTE `segmented`**: `SELECT base.*, ntile($1::int) OVER (ORDER BY pk_cols) AS segment FROM base`.
- **CTE `segment_hashes`**:  
  `SELECT segment, md5(string_agg(md5(row_to_json(segmented)::text), '' ORDER BY pk_cols)) AS segment_hash FROM segmented GROUP BY segment`.
- **CTE `segment_bounds`**: For each segment, `min(pk_i)`, `max(pk_i)` for every PK column (for later narrowing).
- **Final SELECT**: Join hashes and bounds so each row has `segment`, `segment_hash`, `min_*`, `max_*`.

Parameters: `$1` = number of segments \(N\).

**2. Bounded bucket hash (recursive pinpoint)**

- Same structure as above, but **CTE `base`** is restricted with:
  `WHERE (pk1, pk2, ...) >= ($2, $3, ...) AND (pk1, pk2, ...) <= ($k+2, ...)`.
- Parameters: `$1` = number of segments (e.g. 2), then lower bound PK values, then upper bound PK values.
- Used to split a mismatched segment into sub-segments and recurse until the range is small enough to repair in one batch.

**3. Repair**

- Fetch: `SELECT * FROM table WHERE (pk,...) >= (...) AND (pk,...) <= (...) ORDER BY pk_cols`.
- Upsert: `INSERT INTO table (...) VALUES ($1,...) ON CONFLICT (pk_cols) DO UPDATE SET col = EXCLUDED.col, ...`.

Determinism: both sides use the same `ORDER BY pk_cols` in `ntile` and in `string_agg`, so identical data yields the same segment hashes.
