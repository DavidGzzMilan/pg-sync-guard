"""
Control Plane: central schema (syncguard) to track validation runs and
divergence/remediation actions. Uses a separate DB connection so it does
not interfere with Publisher/Subscriber hashing.
"""

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

RUN_STATUS_RUNNING = "running"
RUN_STATUS_SUCCESS = "success"
RUN_STATUS_DIVERGED = "diverged"
RUN_STATUS_FAILED = "failed"


def _to_jsonable(obj: Any) -> Any:
    """Convert a value to a JSON-serializable form for JSONB storage."""
    if obj is None:
        return None
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (UUID, Decimal)):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


def _record_to_dict(record: asyncpg.Record) -> Dict[str, Any]:
    """Convert an asyncpg Record to a dict suitable for JSONB (with serializable values)."""
    return {k: _to_jsonable(record[k]) for k in record.keys()}


class ControlPlane:
    """
    Writes validation run state and divergence logs to the syncguard schema
    on a dedicated control database connection.
    """

    def __init__(self, conn: asyncpg.Connection, schema: str = "syncguard") -> None:
        self.conn = conn
        self.schema = schema
        self._qual = f'"{schema}"'

    def _runs(self) -> str:
        return f'{self._qual}.validation_runs'

    def _log(self) -> str:
        return f'{self._qual}.divergence_log'

    async def start_run(self, table_name: str) -> UUID:
        """
        Insert a new validation run (status=running). Returns run_id.
        table_name should identify the table, e.g. "public.customers".
        """
        sql = f"""
        INSERT INTO {self._runs()} (table_name, status, mismatch_count)
        VALUES ($1, $2, 0)
        RETURNING run_id
        """
        row = await self.conn.fetchrow(sql, table_name, RUN_STATUS_RUNNING)
        run_id = row["run_id"]
        logger.info("Control plane: started run %s for table %s", run_id, table_name)
        return run_id

    async def log_divergence(
        self,
        run_id: UUID,
        pk_value: str,
        publisher_data: Optional[Dict[str, Any]] = None,
        subscriber_data: Optional[Dict[str, Any]] = None,
        repair_sql: Optional[str] = None,
        repaired_at: Optional[datetime] = None,
    ) -> None:
        """
        Insert a row into divergence_log for a repaired divergence.
        repaired_at is set when the repair was applied (typically now()).
        """
        sql = f"""
        INSERT INTO {self._log()} (run_id, pk_value, publisher_data, subscriber_data, repair_sql, repaired_at)
        VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
        """
        pub_json = json.dumps(publisher_data) if publisher_data is not None else None
        sub_json = json.dumps(subscriber_data) if subscriber_data is not None else None
        await self.conn.execute(
            sql, run_id, pk_value, pub_json, sub_json, repair_sql, repaired_at
        )

    async def complete_run(
        self,
        run_id: UUID,
        status: str,
        mismatch_count: int = 0,
    ) -> None:
        """Update the validation run with final status, finished_at, and mismatch_count."""
        sql = f"""
        UPDATE {self._runs()}
        SET status = $1, finished_at = now(), mismatch_count = $2
        WHERE run_id = $3
        """
        await self.conn.execute(sql, status, mismatch_count, run_id)
        logger.info("Control plane: run %s completed with status=%s, mismatch_count=%s", run_id, status, mismatch_count)

    @staticmethod
    def record_to_jsonable(record: asyncpg.Record) -> Dict[str, Any]:
        """Convert an asyncpg Record to a dict for publisher_data/subscriber_data."""
        return _record_to_dict(record)
