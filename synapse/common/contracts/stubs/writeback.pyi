"""Contract stub for synapse/common/writeback.py (owner: T9). See C5."""
from typing import Any, Optional, Sequence
from pyspark.sql import DataFrame
from common.config import PipelineConfig

ALLOWED_SCHEMA: str  # "DATA_PRODUCTS_DB"

def conform_to_contract(df: DataFrame, table: str) -> DataFrame: ...
def truncate_and_load(
    cfg: PipelineConfig,
    df: DataFrame,
    table: str,  # "DATA_PRODUCTS_DB.<TABLE>"
    job_name: str,
    delete_predicate: Optional[str] = None,  # e.g. "REPORTING_PERIOD = ?"; None -> TRUNCATE
    predicate_params: Optional[Sequence[Any]] = None,
    update_stats: bool = True,
) -> int: ...
