"""Contract stub for synapse/common/sql_connector.py (owner: T9). See C4 §3.

Callers use module-attribute access (`from common import sql_connector as sqlc`) so tests can
monkeypatch a single seam.
"""
from contextlib import AbstractContextManager
from typing import Any, Optional, Sequence
from pyspark.sql import DataFrame, SparkSession
from common.config import PipelineConfig

ODBC_DRIVER: str  # "ODBC Driver 18 for SQL Server"
SQL_COPT_SS_ACCESS_TOKEN: int  # 1256
TOKEN_AUDIENCE_SYNAPSE: str  # "DW" (mssparkutils.credentials.getToken)
TOKEN_SCOPE_AZURE_SQL: str  # "https://database.windows.net/.default"

def split_table(table: str) -> tuple[str, str]: ...  # "SCHEMA.TABLE" -> ("SCHEMA", "TABLE"); validates against C1
def get_access_token(cfg: PipelineConfig) -> str: ...
def get_connection(cfg: PipelineConfig) -> AbstractContextManager[Any]: ...  # yields a pyodbc.Connection
def execute_sql(cfg: PipelineConfig, sql: str, params: Optional[Sequence[Any]] = None) -> int: ...  # rowcount
def query_sql(cfg: PipelineConfig, sql: str, params: Optional[Sequence[Any]] = None) -> list[tuple[Any, ...]]: ...
def read_table(
    spark: SparkSession,
    cfg: PipelineConfig,
    table: str,  # "SCHEMA.TABLE" from C1
    columns: Optional[Sequence[str]] = None,
    where: Optional[str] = None,  # applied as a DataFrame filter expression
) -> DataFrame: ...
