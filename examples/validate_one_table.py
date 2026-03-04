#!/usr/bin/env python3
"""
Example: validate and repair a single table between Publisher and Subscriber,
with optional Control Plane (syncguard schema) and progress output.

Env: PUB_DSN, SUB_DSN, CONTROL_DSN (optional), SCHEMA, TABLE.
"""
import asyncio
import os

import asyncpg

from sync_guard import SyncGuard, validate_and_repair, console_progress

PUB_DSN = os.environ.get("PUB_DSN", "postgres://localhost:5432/pubdb")
SUB_DSN = os.environ.get("SUB_DSN", "postgres://localhost:5433/subdb")
CONTROL_DSN = os.environ.get("CONTROL_DSN")  # optional: separate DB for syncguard schema
SCHEMA = os.environ.get("SCHEMA", "public")
TABLE = os.environ.get("TABLE", "my_replicated_table")


async def main() -> None:
    pub = await asyncpg.connect(PUB_DSN)
    sub = await asyncpg.connect(SUB_DSN)
    control_conn = await asyncpg.connect(CONTROL_DSN) if CONTROL_DSN else None

    guard = SyncGuard(pub, sub, num_segments=32)

    repaired = await validate_and_repair(
        guard,
        SCHEMA,
        TABLE,
        control_conn=control_conn,
        progress_callback=console_progress,
    )

    if control_conn:
        await control_conn.close()
    await pub.close()
    await sub.close()


if __name__ == "__main__":
    asyncio.run(main())
