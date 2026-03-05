#!/usr/bin/env python3
"""
SyncGuard entry point: run validation (and optionally repair) for a single table.

Use with the Control Plane + Dashboard:
  - Run with --validate-only so the process only finds and logs divergences;
    repairs can then be applied from the dashboard (Execute Repair) asynchronously.
  - Run without --validate-only for a one-shot validate-and-repair (same process
    applies repairs immediately).

Env: PUB_DSN, SUB_DSN, CONTROL_DSN (optional), SCHEMA, TABLE.
     VALIDATE_ONLY=1 can be used instead of --validate-only.
"""
import argparse
import asyncio
import os
import sys

import asyncpg

# Allow running as python main.py from repo root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sync_guard import SyncGuard, validate_and_repair, validate_only, console_progress


def main() -> None:
    parser = argparse.ArgumentParser(description="SyncGuard: validate (and optionally repair) table sync.")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate and log divergences to the control plane; do not repair. Use with the dashboard to run repairs asynchronously.",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("SCHEMA", "public"),
        help="Table schema (default: SCHEMA env or 'public').",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("TABLE", ""),
        help="Table name (default: TABLE env).",
    )
    parser.add_argument(
        "--segments",
        type=int,
        default=int(os.environ.get("SYNCGUARD_SEGMENTS", "32")),
        help="Number of segments for bucket hash (default: 32).",
    )
    args = parser.parse_args()

    validate_only_mode = args.validate_only or os.environ.get("VALIDATE_ONLY", "").strip() in ("1", "true", "yes")
    table = args.table or os.environ.get("TABLE", "my_replicated_table")
    schema = args.schema

    pub_dsn = os.environ.get("PUB_DSN", "postgres://localhost:5432/pubdb")
    sub_dsn = os.environ.get("SUB_DSN", "postgres://localhost:5433/subdb")
    control_dsn = os.environ.get("CONTROL_DSN")

    async def run() -> None:
        pub = await asyncpg.connect(pub_dsn)
        sub = await asyncpg.connect(sub_dsn)
        control_conn = await asyncpg.connect(control_dsn) if control_dsn else None

        guard = SyncGuard(pub, sub, num_segments=args.segments)

        if validate_only_mode:
            await validate_only(
                guard,
                schema,
                table,
                num_segments=args.segments,
                control_conn=control_conn,
                progress_callback=console_progress,
            )
        else:
            await validate_and_repair(
                guard,
                schema,
                table,
                num_segments=args.segments,
                control_conn=control_conn,
                progress_callback=console_progress,
            )

        if control_conn:
            await control_conn.close()
        await pub.close()
        await sub.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
