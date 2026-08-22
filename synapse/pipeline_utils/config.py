"""Runtime configuration for the Synapse Spark jobs.

Replaces the environment/database references that config/pipeline_config.cfg
exported for the Teradata + SAS pipeline. Database references follow the
Snowflake layout defined by TICKET-01:

    CORE_BANKING_DB   -> RETAIL_BANKING_<ENV>.CORE_BANKING
    TXN_PROCESSING_DB -> RETAIL_BANKING_<ENV>.TXN_PROCESSING
    ETL_STAGING_DB    -> RETAIL_BANKING_<ENV>.ETL_STAGING
    DATA_PRODUCTS_DB  -> RETAIL_BANKING_<ENV>.DATA_PRODUCTS
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_ENV = "DEV"
RUN_LOG_TABLE = "PIPELINE_RUN_LOG"


@dataclass(frozen=True)
class SnowflakeConfig:
    """Connection coordinates for Snowflake (secret material excluded)."""

    account: str
    user: str
    role: str
    warehouse: str
    database: str
    private_key_secret: str

    @property
    def core_banking_schema(self) -> str:
        return "CORE_BANKING"

    @property
    def txn_processing_schema(self) -> str:
        return "TXN_PROCESSING"

    @property
    def staging_schema(self) -> str:
        return "ETL_STAGING"

    @property
    def data_products_schema(self) -> str:
        return "DATA_PRODUCTS"


@dataclass(frozen=True)
class PipelineConfig:
    """Everything a Synapse Spark job needs to locate its inputs and secrets."""

    env: str
    key_vault_url: str
    snowflake: SnowflakeConfig
    run_log_schema: str = "ETL_STAGING"
    run_log_table: str = RUN_LOG_TABLE

    def qualified(self, schema: str, table: str) -> str:
        return f"{self.snowflake.database}.{schema}.{table}"

    @property
    def run_log_fqn(self) -> str:
        return self.qualified(self.run_log_schema, self.run_log_table)


def _require(env: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = env.get(name, default)
    if value is None or value == "":
        raise KeyError(
            f"Missing required configuration value '{name}'. "
            "Set it on the Synapse Spark pool or in the job's environment."
        )
    return value


def load_config(env: Mapping[str, str] | None = None) -> PipelineConfig:
    """Build a :class:`PipelineConfig` from environment variables.

    No credential material is read here: only the *name* of the Key Vault
    secret holding the Snowflake private key is configured, the value itself is
    resolved at connection time (see :mod:`pipeline_utils.secrets`).
    """
    env = os.environ if env is None else env
    environment = env.get("PIPELINE_ENV", DEFAULT_ENV).upper()

    snowflake = SnowflakeConfig(
        account=_require(env, "SNOWFLAKE_ACCOUNT"),
        user=_require(env, "SNOWFLAKE_USER"),
        role=env.get("SNOWFLAKE_ROLE", "RETAIL_BANKING_TRANSFORMER"),
        warehouse=env.get("SNOWFLAKE_WAREHOUSE", "WH_SPARK_XS"),
        database=env.get("SNOWFLAKE_DATABASE", f"RETAIL_BANKING_{environment}"),
        private_key_secret=env.get(
            "SNOWFLAKE_PRIVATE_KEY_SECRET", "snowflake-synapse-private-key"
        ),
    )

    return PipelineConfig(
        env=environment,
        key_vault_url=_require(env, "AZURE_KEY_VAULT_URL"),
        snowflake=snowflake,
    )
