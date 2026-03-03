#!/usr/bin/env python3
"""
Example: validate and optionally repair a single table between
Publisher and Subscriber. Set PUB_DSN and SUB_DSN (or pass as env).
"""
import asyncio
import os

import asyncpg

from sync_guard import SyncGuard, validate_and_repair

PUB_DSN = os.environ.get("PUB_DSN", "postgres://localhost:5432/pubdb")
SUB_DSN = os.environ.get("SUB_DSN", "postgres://localhost:5433/subdb")
SCHEMA = os.environ.get("SCHEMA", "public")
TABLE = os.environ.get("TABLE", "my_replicated_table")


async def main() -> None:
    pub = await asyncpg.connect(PUB_DSN)
    sub = await asyncpg.connect(SUB_DSN)
    guard = SyncGuard(pub, sub, num_segments=32)
    # Validate only
    mismatches = await guard.validate(SCHEMA, TABLE)
    if not mismatches:
        print("OK: no mismatches")
    else:
        print(f"Mismatches: {len(mismatches)} segment(s)")
        # Optional: repair
        repaired = await validate_and_repair(guard, SCHEMA, TABLE)
        print(f"Repaired: {len(repaired)} row(s)", repaired[:5], "..." if len(repaired) > 5 else "")
    await pub.close()
    await sub.close()


if __name__ == "__main__":
    asyncio.run(main())
