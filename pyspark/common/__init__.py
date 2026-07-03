"""Shared building blocks for the PySpark retail-banking analytics pipeline.

Ports the reusable SAS/BTEQ infrastructure:
* :mod:`common.config`     -- pipeline_config.cfg loader (LOOKBACK_MONTHS, etc.)
* :mod:`common.spark`      -- SparkSession factory with AQE/skew handling
* :mod:`common.io`         -- readers/writers replacing connect_teradata
* :mod:`common.schemas`    -- DDL output contracts
* :mod:`common.validation` -- validate_table's three checks
* :mod:`common.audit`      -- log_step + PIPELINE_AUDIT/ETL_RUN_LOG
* :mod:`common.dates`      -- Teradata date arithmetic in Spark
"""

from .audit import AuditLog, get_logger
from .config import MODEL_VERSIONS, PipelineConfig, add_months
from .io import DataIO, JdbcDataIO, LocalDataIO, local_io
from .spark import build_local_spark, build_spark
from .validation import ValidationError, ValidationResult, abort_on_failure, validate_table

__all__ = [
    "AuditLog",
    "get_logger",
    "MODEL_VERSIONS",
    "PipelineConfig",
    "add_months",
    "DataIO",
    "JdbcDataIO",
    "LocalDataIO",
    "local_io",
    "build_spark",
    "build_local_spark",
    "ValidationError",
    "ValidationResult",
    "abort_on_failure",
    "validate_table",
]
