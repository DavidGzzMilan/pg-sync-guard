"""
SyncGuard: parallel bucket-hash validation and recursive pinpoint/repair.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, List, Optional, Tuple
from uuid import UUID

import asyncpg

from sync_guard.control_plane import (
    ControlPlane,
    RUN_STATUS_DIVERGED,
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
)
from sync_guard.exceptions import RepairError, SchemaError
from sync_guard.hash_queries import (
    build_bucket_hash_query,
    build_bucket_hash_query_with_bounds,
    build_fetch_row_query,
    build_row_count_query,
    build_upsert_sql,
)
from sync_guard.schema import TableInfo, analyze_table

logger = logging.getLogger(__name__)

# Optional callback (stage: str, detail: str) for human-readable progress (e.g. print to console)
ProgressCallback = Callable[[str, str], None]


def console_progress(stage: str, detail: str) -> None:
    """Default progress callback that prints to stdout with a [SyncGuard] prefix."""
    print(f"[SyncGuard] {stage}: {detail}")

# Default segment count for initial validation
DEFAULT_NUM_SEGMENTS = 32
# When narrowing, split into this many sub-segments
PINPOINT_SEGMENTS = 2
# Stop recursion when segment has at most this many rows and repair all
REPAIR_BATCH_THRESHOLD = 10


class SyncGuard:
    """
    Validates data consistency between a Logical Replication Publisher and
    Subscriber by bucket hashing, then pinpoints and repairs mismatches.
    """

    def __init__(
        self,
        publisher_conn: asyncpg.Connection,
        subscriber_conn: asyncpg.Connection,
        *,
        num_segments: int = DEFAULT_NUM_SEGMENTS,
        pinpoint_segments: int = PINPOINT_SEGMENTS,
        repair_batch_threshold: int = REPAIR_BATCH_THRESHOLD,
    ) -> None:
        self.publisher = publisher_conn
        self.subscriber = subscriber_conn
        self.num_segments = num_segments
        self.pinpoint_segments = pinpoint_segments
        self.repair_batch_threshold = repair_batch_threshold
        self._table_info: Optional[TableInfo] = None

    async def analyze_schema(self, schema: str, table: str) -> TableInfo:
        """
        Inspect the target table on the Publisher: resolve Primary Key and
        high-churn columns. Caches result for subsequent validate/repair.
        """
        self._table_info = await analyze_table(self.publisher, schema, table)
        logger.info(
            "Schema analyzed: %s PK=%s high_churn=%s",
            self._table_info.qualified_name,
            self._table_info.primary_key_columns,
            self._table_info.high_churn_columns,
        )
        return self._table_info

    @property
    def table_info(self) -> TableInfo:
        if self._table_info is None:
            raise SchemaError("Call analyze_schema first")
        return self._table_info

    def _segment_bounds_from_row(self, row: asyncpg.Record) -> Tuple[tuple, tuple]:
        """Extract (pk_lower, pk_upper) from a segment hash result row (with b.*)."""
        pk_names = self.table_info.primary_key_columns
        lower = tuple(row[f"min_{n}"] for n in pk_names)
        upper = tuple(row[f"max_{n}"] for n in pk_names)
        return lower, upper

    async def _run_bucket_hash_both(
        self,
        num_segments: int,
        pk_lower: Optional[Tuple[Any, ...]] = None,
        pk_upper: Optional[Tuple[Any, ...]] = None,
    ) -> Tuple[List[asyncpg.Record], List[asyncpg.Record]]:
        """Run the bucket hash query on both connections in parallel."""
        info = self.table_info
        if pk_lower is not None and pk_upper is not None:
            sql = build_bucket_hash_query_with_bounds(
                info, num_segments, pk_lower, pk_upper
            )
            args = (num_segments, *pk_lower, *pk_upper)
        else:
            sql = build_bucket_hash_query(info, num_segments)
            args = (num_segments,)

        pub_task = self.publisher.fetch(sql, *args)
        sub_task = self.subscriber.fetch(sql, *args)
        pub_rows, sub_rows = await asyncio.gather(pub_task, sub_task)
        return list(pub_rows), list(sub_rows)

    def _compare_hashes(
        self,
        pub_rows: List[asyncpg.Record],
        sub_rows: List[asyncpg.Record],
    ) -> List[Tuple[asyncpg.Record, Optional[asyncpg.Record]]]:
        """
        Compare segment hashes. Returns list of (pub_row, sub_row) for segments
        where hash differs (sub_row may be None if segment missing on subscriber).
        """
        sub_by_segment = {r["segment"]: r for r in sub_rows}
        mismatches: List[Tuple[asyncpg.Record, Optional[asyncpg.Record]]] = []
        for pr in pub_rows:
            seg = pr["segment"]
            sr = sub_by_segment.get(seg)
            if sr is None or pr["segment_hash"] != sr["segment_hash"]:
                mismatches.append((pr, sr))
        return mismatches

    async def validate(
        self,
        schema: str,
        table: str,
        *,
        num_segments: Optional[int] = None,
    ) -> List[Tuple[asyncpg.Record, Optional[asyncpg.Record]]]:
        """
        Run bucket-hash validation on the table. Call analyze_schema if not
        already done. Returns list of (publisher_segment_row, subscriber_segment_row)
        for each segment with a hash mismatch.
        """
        if self._table_info is None or (
            self._table_info.schema_name != schema
            or self._table_info.table_name != table
        ):
            await self.analyze_schema(schema, table)
        n = num_segments if num_segments is not None else self.num_segments
        pub_rows, sub_rows = await self._run_bucket_hash_both(n)
        return self._compare_hashes(pub_rows, sub_rows)

    async def _row_count_in_range(self, pk_lower: tuple, pk_upper: tuple) -> int:
        """Count rows in the given PK range on the publisher."""
        sql = build_row_count_query(self.table_info, pk_lower, pk_upper)
        row = await self.publisher.fetchrow(sql, *pk_lower, *pk_upper)
        return row["cnt"] if row else 0

    async def _pinpoint_and_repair(
        self,
        pk_lower: tuple,
        pk_upper: tuple,
        repaired: List[tuple],
        *,
        run_id: Optional[UUID] = None,
        control_plane: Optional[ControlPlane] = None,
        progress: Optional[ProgressCallback] = None,
    ) -> None:
        """
        Recursively narrow the range until we have at most REPAIR_BATCH_THRESHOLD
        rows, then fetch from publisher and upsert on subscriber.
        """
        info = self.table_info
        count = await self._row_count_in_range(pk_lower, pk_upper)
        if count == 0:
            return
        if count <= self.repair_batch_threshold:
            await self._repair_range(
                pk_lower,
                pk_upper,
                repaired,
                run_id=run_id,
                control_plane=control_plane,
                progress=progress,
            )
            return

        # Split and see which sub-segment(s) differ
        sql = build_bucket_hash_query_with_bounds(
            info, self.pinpoint_segments, pk_lower, pk_upper
        )
        args = (self.pinpoint_segments, *pk_lower, *pk_upper)
        pub_rows, sub_rows = await asyncio.gather(
            self.publisher.fetch(sql, *args),
            self.subscriber.fetch(sql, *args),
        )
        pub_list = list(pub_rows)
        sub_list = list(sub_rows)
        sub_by_seg = {r["segment"]: r for r in sub_list}
        for pr in pub_list:
            seg = pr["segment"]
            sr = sub_by_seg.get(seg)
            if sr is None or pr["segment_hash"] != sr["segment_hash"]:
                seg_lower, seg_upper = self._segment_bounds_from_row(pr)
                await self._pinpoint_and_repair(
                    seg_lower,
                    seg_upper,
                    repaired,
                    run_id=run_id,
                    control_plane=control_plane,
                    progress=progress,
                )

    async def _repair_range(
        self,
        pk_lower: tuple,
        pk_upper: tuple,
        repaired: List[tuple],
        *,
        run_id: Optional[UUID] = None,
        control_plane: Optional[ControlPlane] = None,
        progress: Optional[ProgressCallback] = None,
    ) -> None:
        """
        Fetch all rows in the PK range from the publisher and upsert each
        on the subscriber. Optionally log each repair to the control plane.
        """
        info = self.table_info
        pk_quoted = info.pk_columns_quoted()
        pk_tuple_sql = ", ".join(pk_quoted)
        n = len(pk_quoted)
        placeholders_low = ", ".join(f"${i+1}" for i in range(n))
        placeholders_high = ", ".join(f"${i + 1 + n}" for i in range(n))
        where = f"({pk_tuple_sql}) >= ({placeholders_low}) AND ({pk_tuple_sql}) <= ({placeholders_high})"
        qual = info.qualified_name
        fetch_sql = f"SELECT * FROM {qual} WHERE {where} ORDER BY {info.pk_order_clause()}"
        rows = await self.publisher.fetch(fetch_sql, *pk_lower, *pk_upper)
        upsert_sql = build_upsert_sql(info)
        fetch_row_sql = build_fetch_row_query(info)
        col_names = [c.name for c in info.columns]
        pk_names = info.primary_key_columns

        for row in rows:
            pk_vals = tuple(row[c] for c in pk_names)
            values = tuple(row[c] for c in col_names)

            # Optionally capture subscriber row before repair (for divergence_log)
            sub_row = None
            if control_plane and run_id:
                sub_row = await self.subscriber.fetchrow(fetch_row_sql, *pk_vals)

            try:
                await self.subscriber.execute(upsert_sql, *values)
            except Exception as e:
                raise RepairError(f"Upsert failed for PK {pk_vals}: {e}") from e

            repaired.append(pk_vals)
            now = datetime.now(timezone.utc)

            if control_plane and run_id:
                pk_value_str = json.dumps(list(pk_vals))
                pub_data = ControlPlane.record_to_jsonable(row)
                sub_data = ControlPlane.record_to_jsonable(sub_row) if sub_row else None
                await control_plane.log_divergence(
                    run_id=run_id,
                    pk_value=pk_value_str,
                    publisher_data=pub_data,
                    subscriber_data=sub_data,
                    repair_sql=upsert_sql,
                    repaired_at=now,
                )

            if progress:
                progress("repaired", f"PK {pk_vals}")

        logger.info("Repaired %d row(s) in range %s..%s", len(rows), pk_lower, pk_upper)


async def validate_and_repair(
    guard: SyncGuard,
    schema: str,
    table: str,
    *,
    num_segments: Optional[int] = None,
    control_conn: Optional[asyncpg.Connection] = None,
    control_plane: Optional[ControlPlane] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> List[tuple]:
    """
    Run validation; for every mismatched segment, recursively pinpoint and
    repair. Returns list of repaired primary key tuples.

    If control_conn or control_plane is provided, writes to the syncguard
    schema: inserts a validation run at start, logs each divergence to
    divergence_log, and updates the run (status, finished_at, mismatch_count)
    at end. Use a separate connection for the control DB so it does not
    interfere with Publisher/Subscriber.

    progress_callback(stage, detail) is called at key steps for human-readable
    output (e.g. print to console).
    """
    table_name = f"{schema}.{table}"
    cp = control_plane if control_plane is not None else (ControlPlane(control_conn) if control_conn else None)
    run_id: Optional[UUID] = None
    if cp:
        run_id = await cp.start_run(table_name)
        if progress_callback:
            progress_callback("started", f"Run {run_id} for table {table_name}")

    def progress(stage: str, detail: str) -> None:
        if progress_callback:
            progress_callback(stage, detail)

    try:
        if progress_callback:
            progress_callback("schema", f"Analyzing table {table_name}...")
        mismatches = await guard.validate(schema, table, num_segments=num_segments)

        if progress_callback:
            if not mismatches:
                progress_callback("validation", "All segments match. No divergence.")
            else:
                progress_callback("validation", f"Found {len(mismatches)} segment(s) with hash mismatch.")

        repaired: List[tuple] = []
        if mismatches and progress_callback:
            progress_callback("repair", "Pinpointing and repairing diverged rows...")

        for pub_row, _ in mismatches:
            pk_lower, pk_upper = guard._segment_bounds_from_row(pub_row)
            await guard._pinpoint_and_repair(
                pk_lower,
                pk_upper,
                repaired,
                run_id=run_id,
                control_plane=cp,
                progress=progress_callback,
            )

        if cp and run_id is not None:
            status = RUN_STATUS_DIVERGED if repaired else RUN_STATUS_SUCCESS
            await cp.complete_run(run_id, status=status, mismatch_count=len(repaired))

        if progress_callback:
            if repaired:
                progress_callback("done", f"Run finished: status={'diverged' if repaired else 'success'}, repaired {len(repaired)} row(s). PKs: {repaired[:10]}{'...' if len(repaired) > 10 else ''}")
            else:
                progress_callback("done", "Run finished: status=success, no repairs needed.")

        return repaired
    except Exception as e:
        if cp and run_id is not None:
            await cp.complete_run(run_id, status=RUN_STATUS_FAILED, mismatch_count=0)
        if progress_callback:
            progress_callback("error", str(e))
        logger.exception("validate_and_repair failed")
        raise
