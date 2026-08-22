"""Shared utilities for the Azure Synapse Spark jobs.

This package replaces the SAS macro library (``sas/macros``):

    %connect_teradata -> pipeline_utils.snowflake_io.SnowflakeIO
                         + pipeline_utils.secrets (Azure Key Vault)
    %log_step         -> pipeline_utils.run_log.RunLogger
    %validate_table   -> pipeline_utils.validation.validate_table
"""

from pipeline_utils.config import PipelineConfig, SnowflakeConfig, load_config
from pipeline_utils.run_log import RunLogEntry, RunLogger
from pipeline_utils.secrets import (
    KeyVaultSecretResolver,
    MappingSecretResolver,
    SecretResolver,
)
from pipeline_utils.snowflake_io import SnowflakeIO
from pipeline_utils.validation import (
    CheckResult,
    ValidationError,
    ValidationReport,
    validate_table,
)

__all__ = [
    "CheckResult",
    "KeyVaultSecretResolver",
    "MappingSecretResolver",
    "PipelineConfig",
    "RunLogEntry",
    "RunLogger",
    "SecretResolver",
    "SnowflakeConfig",
    "SnowflakeIO",
    "ValidationError",
    "ValidationReport",
    "load_config",
    "validate_table",
]
