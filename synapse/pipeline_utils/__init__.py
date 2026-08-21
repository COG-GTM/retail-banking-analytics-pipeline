"""Shared utilities for the Azure Synapse Spark jobs of the retail banking pipeline.

This package replaces the shared SAS macro library:

    sas/macros/connect_teradata.sas  ->  pipeline_utils.snowflake_io
    sas/macros/log_step.sas          ->  pipeline_utils.run_log
    sas/macros/validate_table.sas    ->  pipeline_utils.validation
"""

from pipeline_utils.config import PipelineConfig, SnowflakeConfig
from pipeline_utils.run_log import RunLogger, RunLogRecord, Status
from pipeline_utils.secrets import SecretResolver
from pipeline_utils.snowflake_io import SnowflakeIO
from pipeline_utils.validation import (
    ValidationError,
    ValidationResult,
    validate_dataframe,
)

__all__ = [
    "PipelineConfig",
    "SnowflakeConfig",
    "RunLogger",
    "RunLogRecord",
    "Status",
    "SecretResolver",
    "SnowflakeIO",
    "ValidationError",
    "ValidationResult",
    "validate_dataframe",
]
