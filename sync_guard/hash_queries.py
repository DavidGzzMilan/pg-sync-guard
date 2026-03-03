"""
Recursive SQL for bucket hashing and pinpointing mismatches.

- Bucket hash: CTE segments table into N buckets by PK order, then computes
  a deterministic per-segment hash using md5(row_to_json(...)) aggregated
  with string_agg(..., '' ORDER BY pk_cols) so both sides produce the same
  hash when data matches.
- Pinpoint: same pattern restricted to a PK range, with N=2 (or more) to
  recursively narrow until the segment contains a single row (or small set)
  for repair.
"""

from sync_guard.schema import TableInfo


def build_bucket_hash_query(info: TableInfo, num_segments: int) -> str:
    """
    Build the bucket-hash query for the table. Returns (segment, segment_hash)
    with optional PK bounds per segment for narrowing.

    Uses:
      - ntile($N) OVER (ORDER BY pk_cols) to assign segment id
      - md5(string_agg(md5(row_to_json(t)::text), '' ORDER BY pk_cols)) for
        deterministic per-segment hash (same order on both sides).
    """
    qual = info.qualified_name
    order_clause = info.pk_order_clause()
    pk_quoted = info.pk_columns_quoted()

    # Per-segment min/max PK for recursive narrowing (single PK: one min/max; composite: tuple)
    # We select min/max for each PK column per segment so we can filter later.
    min_max_selects = [f"min({c}) AS min_{c.strip('\"')}" for c in pk_quoted]
    min_max_selects += [f"max({c}) AS max_{c.strip('\"')}" for c in pk_quoted]
    min_max_clause = ", ".join(min_max_selects)

    sql = f"""
WITH base AS (
    SELECT * FROM {qual}
),
segmented AS (
    SELECT base.*, ntile($1::int) OVER (ORDER BY {order_clause}) AS segment
    FROM base
),
segment_hashes AS (
    SELECT
        segment,
        md5(string_agg(md5(row_to_json(segmented)::text), '' ORDER BY {order_clause})) AS segment_hash
    FROM segmented
    GROUP BY segment
),
segment_bounds AS (
    SELECT segment, {min_max_clause}
    FROM segmented
    GROUP BY segment
)
SELECT
    h.segment,
    h.segment_hash,
    b.*
FROM segment_hashes h
JOIN segment_bounds b ON b.segment = h.segment
ORDER BY h.segment
"""
    return sql.strip()


def build_bucket_hash_query_with_bounds(
    info: TableInfo,
    num_segments: int,
    pk_lower: tuple,
    pk_upper: tuple,
) -> str:
    """
    Same as build_bucket_hash_query but restricted to rows where
    (pk1, pk2, ...) >= pk_lower and (pk1, pk2, ...) <= pk_upper.
    Used for recursive pinpointing inside a segment that had a mismatch.
    """
    qual = info.qualified_name
    order_clause = info.pk_order_clause()
    pk_quoted = info.pk_columns_quoted()
    pk_names = [c.strip('"') for c in pk_quoted]

    if len(pk_names) != len(pk_lower) or len(pk_names) != len(pk_upper):
        raise ValueError("pk_lower/pk_upper length must match primary key columns")

    # WHERE (pk1, pk2, ...) >= ($2, $3, ...) AND (pk1, pk2, ...) <= ($k+2, ...)
    n = len(pk_names)
    placeholders_low = ", ".join(f"${i+2}" for i in range(n))
    placeholders_high = ", ".join(f"${i + 2 + n}" for i in range(n))
    pk_tuple = ", ".join(pk_quoted)
    where_clause = f"({pk_tuple}) >= ({placeholders_low}) AND ({pk_tuple}) <= ({placeholders_high})"

    min_max_selects = [f"min({c}) AS min_{name}" for c, name in zip(pk_quoted, pk_names)]
    min_max_selects += [f"max({c}) AS max_{name}" for c, name in zip(pk_quoted, pk_names)]
    min_max_clause = ", ".join(min_max_selects)

    # $1 = num_segments, $2..$n+1 = pk_lower, $n+2..$2n+1 = pk_upper
    sql = f"""
WITH base AS (
    SELECT * FROM {qual}
    WHERE {where_clause}
),
segmented AS (
    SELECT base.*, ntile($1::int) OVER (ORDER BY {order_clause}) AS segment
    FROM base
),
segment_hashes AS (
    SELECT
        segment,
        md5(string_agg(md5(row_to_json(segmented)::text), '' ORDER BY {order_clause})) AS segment_hash
    FROM segmented
    GROUP BY segment
),
segment_bounds AS (
    SELECT segment, {min_max_clause}
    FROM segmented
    GROUP BY segment
)
SELECT
    h.segment,
    h.segment_hash,
    b.*
FROM segment_hashes h
JOIN segment_bounds b ON b.segment = h.segment
ORDER BY h.segment
"""
    return sql.strip()


def build_row_count_query(info: TableInfo, pk_lower: tuple, pk_upper: tuple) -> str:
    """Count rows in the given PK range (for knowing when to stop recursion)."""
    qual = info.qualified_name
    pk_quoted = info.pk_columns_quoted()
    pk_tuple = ", ".join(pk_quoted)
    n = len(pk_quoted)
    placeholders_low = ", ".join(f"${i+1}" for i in range(n))
    placeholders_high = ", ".join(f"${i + 1 + n}" for i in range(n))
    where = f"({pk_tuple}) >= ({placeholders_low}) AND ({pk_tuple}) <= ({placeholders_high})"
    return f"SELECT count(*) AS cnt FROM {qual} WHERE {where}"


def build_fetch_row_query(info: TableInfo) -> str:
    """Fetch a single row by primary key; use with conn.fetchrow. Placeholders $1, $2, ... for PK values."""
    qual = info.qualified_name
    pk_quoted = info.pk_columns_quoted()
    n = len(pk_quoted)
    conditions = " AND ".join(f"{c} = ${i+1}" for i, c in enumerate(pk_quoted))
    return f"SELECT * FROM {qual} WHERE {conditions}"


def build_upsert_sql(info: TableInfo) -> str:
    """
    Generate INSERT ... ON CONFLICT DO UPDATE for the table.
    Caller must pass column values in order (all columns) and use
    execute with a tuple; or we return a statement that uses $1, $2, ...
    for column order (same as information_schema order).
    """
    qual = info.qualified_name
    all_cols = [f'"{c.name}"' for c in info.columns]
    pk_cols = info.pk_columns_quoted()
    cols_list = ", ".join(all_cols)
    placeholders = ", ".join(f"${i+1}" for i in range(len(all_cols)))
    conflict_cols = ", ".join(pk_cols)
    non_pk = [c for c in all_cols if c not in pk_cols]
    set_clauses = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk) if non_pk else ", ".join(f"{c} = EXCLUDED.{c}" for c in all_cols)

    return f"""
INSERT INTO {qual} ({cols_list})
VALUES ({placeholders})
ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clauses}
""".strip()
