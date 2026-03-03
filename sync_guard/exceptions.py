"""SyncGuard exceptions."""


class SyncGuardError(Exception):
    """Base exception for SyncGuard."""


class SchemaError(SyncGuardError):
    """Schema inspection or missing primary key."""


class ValidationError(SyncGuardError):
    """Hash mismatch or validation failure."""


class RepairError(SyncGuardError):
    """Repair (INSERT ... ON CONFLICT) failed."""
