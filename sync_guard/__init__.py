"""
SyncGuard: Validate data consistency between PostgreSQL Logical Replication
Publisher and Subscriber using bucket hashing and recursive pinpoint/repair.
"""

from sync_guard.validator import SyncGuard, validate_and_repair
from sync_guard.schema import TableInfo
from sync_guard.exceptions import (
    SyncGuardError,
    SchemaError,
    ValidationError,
    RepairError,
)

__all__ = [
    "SyncGuard",
    "validate_and_repair",
    "TableInfo",
    "SyncGuardError",
    "SchemaError",
    "ValidationError",
    "RepairError",
]
