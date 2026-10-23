"""Contract stub for synapse/common/validation.py (owner: T9). See C3."""
from dataclasses import dataclass, field
from typing import Optional, Sequence
from pyspark.sql import DataFrame
from common.config import PipelineConfig

@dataclass
class ValidationResult:
    table_label: str
    row_count: int
    duplicate_key_groups: int
    null_counts: dict[str, int] = field(default_factory=dict)
    passed: bool = ...
    warnings: list[str] = field(default_factory=list)

class ValidationError(Exception):
    result: ValidationResult
    def __init__(self, message: str, result: ValidationResult) -> None: ...

def default_min_rows(env: str) -> int: ...  # 1000 for UAT/PROD, else 1
def validate_dataframe(
    cfg: PipelineConfig,
    df: DataFrame,
    table_label: str,
    job_name: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: Optional[int] = None,
    raise_on_fail: bool = True,
) -> ValidationResult: ...
def validate_sql_table(
    cfg: PipelineConfig,
    table: str,  # "SCHEMA.TABLE"
    job_name: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: Optional[int] = None,
    raise_on_fail: bool = True,
) -> ValidationResult: ...
