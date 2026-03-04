"""
Schema analysis: discover Primary Key and high-churn columns on the Publisher.
"""

from dataclasses import dataclass, field
from typing import List

from sync_guard.exceptions import SchemaError

# Types often associated with high churn (frequently updated)
HIGH_CHURN_TYPES = {
    "timestamp with time zone",
    "timestamp without time zone",
    "timestamptz",
    "timestamp",
    "integer",
    "bigint",
    "smallint",
    "boolean",
    "numeric",
    "real",
    "double precision",
}


@dataclass
class ColumnInfo:
    """Metadata for a table column."""

    name: str
    data_type: str
    is_primary_key: bool
    ordinal_position: int
    is_high_churn: bool = False
    is_generated_always: bool = False  # GENERATED ALWAYS AS IDENTITY

    def __post_init__(self) -> None:
        self.is_high_churn = self.data_type.lower() in HIGH_CHURN_TYPES


@dataclass
class TableInfo:
    """
    Result of schema analysis: PK columns (ordered) and all columns.
    High-churn columns are flagged for optional use in hashing or reporting.
    """

    schema_name: str
    table_name: str
    primary_key_columns: List[str]
    columns: List[ColumnInfo] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        return f'"{self.schema_name}"."{self.table_name}"'

    @property
    def high_churn_columns(self) -> List[str]:
        return [c.name for c in self.columns if c.is_high_churn and not c.is_primary_key]

    @property
    def has_generated_always_identity(self) -> bool:
        """True if any column is GENERATED ALWAYS AS IDENTITY (requires OVERRIDING SYSTEM VALUE on insert)."""
        return any(c.is_generated_always for c in self.columns)

    def pk_order_clause(self) -> str:
        """ORDER BY clause for deterministic row ordering (PK columns)."""
        return ", ".join(f'"{c}"' for c in self.primary_key_columns)

    def pk_columns_quoted(self) -> List[str]:
        return [f'"{c}"' for c in self.primary_key_columns]


async def analyze_table(conn, schema: str, table: str) -> TableInfo:
    """
    Inspect the table on the given asyncpg connection; return TableInfo
    with primary key and column list. Raises SchemaError if no PK.
    """
    # Resolve schema if not provided
    schema = schema or "public"

    # Primary key columns in order (pg_index + pg_attribute)
    pk_sql = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND a.attnum > 0 AND NOT a.attisdropped
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
      AND i.indisprimary
    ORDER BY array_position(i.indkey, a.attnum)
    """
    pk_rows = await conn.fetch(pk_sql, schema, table)
    primary_key_columns = [r["attname"] for r in pk_rows]

    if not primary_key_columns:
        raise SchemaError(f'Table "{schema}"."{table}" has no primary key')

    # All columns with types and identity (attidentity: 'a' = GENERATED ALWAYS, 'd' = BY DEFAULT)
    cols_sql = """
    SELECT a.attname AS name,
           pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
           a.attnum AS ordinal_position,
           a.attidentity = 'a' AS is_generated_always
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
    """
    col_rows = await conn.fetch(cols_sql, schema, table)
    pk_set = set(primary_key_columns)
    columns = [
        ColumnInfo(
            name=r["name"],
            data_type=r["data_type"],
            is_primary_key=r["name"] in pk_set,
            ordinal_position=r["ordinal_position"],
            is_generated_always=r["is_generated_always"],
        )
        for r in col_rows
    ]

    return TableInfo(
        schema_name=schema,
        table_name=table,
        primary_key_columns=primary_key_columns,
        columns=columns,
    )
