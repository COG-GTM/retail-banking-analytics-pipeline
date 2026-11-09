"""Contract stub for synapse/common/schema.py (owner: T9). Machine-readable form of C1.

Must agree column-for-column with 01_schema_contract.md and T2's DDL.
"""
from dataclasses import dataclass
from typing import Optional
from pyspark.sql.types import DataType, StructType

@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str  # e.g. "DECIMAL(18,2)", "VARCHAR(60)", "DATETIME2(6)"
    nullable: bool
    default: Optional[str] = None

@dataclass(frozen=True)
class TableSpec:
    schema: str
    table: str
    columns: tuple[ColumnSpec, ...]
    distribution: str  # "HASH(CUSTOMER_ID)" | "REPLICATE" | "ROUND_ROBIN"
    @property
    def full_name(self) -> str: ...

TABLES: dict[str, TableSpec]  # keyed by "SCHEMA.TABLE"

def get_table_spec(table: str) -> TableSpec: ...
def column_names(table: str) -> list[str]: ...
def spark_type(sql_type: str) -> DataType: ...
def spark_schema(table: str) -> StructType: ...
