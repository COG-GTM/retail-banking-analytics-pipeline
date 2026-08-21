"""Pipeline configuration.

Replaces the shell-sourced ``config/pipeline_config.cfg`` values that the SAS
programs relied on. Values arrive as Synapse pipeline parameters (surfaced as
environment variables on the Spark job) and never contain secrets: credentials
are resolved at runtime from Azure Key Vault (see :mod:`pipeline_utils.secrets`).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

DEFAULT_ENVIRONMENT = "DEV"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class SnowflakeConfig:
    """Connection settings for Snowflake.

    Mirrors what ``%connect_teradata`` used to hardcode (server, user, LDAP
    password, four LIBNAMEs), but the private key is fetched from Key Vault and
    the LIBNAMEs become fully qualified ``database.schema`` references.
    """

    account: str
    user: str
    role: str
    warehouse: str
    staging_database: str
    staging_schema: str
    data_products_database: str
    data_products_schema: str
    private_key_secret_name: str
    key_vault_url: str
    authenticator: str = "SNOWFLAKE_JWT"

    @classmethod
    def from_env(cls, environment: str = DEFAULT_ENVIRONMENT) -> "SnowflakeConfig":
        suffix = environment.upper()
        return cls(
            account=_env("SNOWFLAKE_ACCOUNT", ""),
            user=_env("SNOWFLAKE_USER", "SVC_SYNAPSE_SPARK"),
            role=_env("SNOWFLAKE_ROLE", "TRANSFORMER"),
            warehouse=_env("SNOWFLAKE_WAREHOUSE", "WH_SPARK"),
            staging_database=_env("SNOWFLAKE_STAGING_DB", f"ETL_STAGING_{suffix}"),
            staging_schema=_env("SNOWFLAKE_STAGING_SCHEMA", "STAGING"),
            data_products_database=_env(
                "SNOWFLAKE_DATA_PRODUCTS_DB", f"DATA_PRODUCTS_{suffix}"
            ),
            data_products_schema=_env("SNOWFLAKE_DATA_PRODUCTS_SCHEMA", "DATA_PRODUCTS"),
            private_key_secret_name=_env(
                "SNOWFLAKE_PRIVATE_KEY_SECRET", "snowflake-synapse-private-key"
            ),
            key_vault_url=_env("AZURE_KEY_VAULT_URL", ""),
        )

    def staging_table(self, table: str) -> str:
        return f"{self.staging_database}.{self.staging_schema}.{table}"

    def data_product_table(self, table: str) -> str:
        return f"{self.data_products_database}.{self.data_products_schema}.{table}"


@dataclass(frozen=True)
class PipelineConfig:
    """Run-level parameters shared by every Synapse Spark job."""

    environment: str = DEFAULT_ENVIRONMENT
    run_id: str = ""
    run_date: date = field(default_factory=date.today)
    log_level: str = "INFO"
    run_log_table: str = "PIPELINE_RUN_LOG"
    snowflake: SnowflakeConfig = field(
        default_factory=lambda: SnowflakeConfig.from_env(DEFAULT_ENVIRONMENT)
    )

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        environment = _env("PIPELINE_ENV", DEFAULT_ENVIRONMENT)
        run_date_raw = _env("RUN_DATE", date.today().isoformat())
        return cls(
            environment=environment,
            run_id=_env("PIPELINE_RUN_ID", ""),
            run_date=date.fromisoformat(run_date_raw),
            log_level=_env("LOG_LEVEL", "INFO"),
            run_log_table=_env("PIPELINE_RUN_LOG_TABLE", "PIPELINE_RUN_LOG"),
            snowflake=SnowflakeConfig.from_env(environment),
        )

    @property
    def run_log_fqn(self) -> str:
        return self.snowflake.data_product_table(self.run_log_table)
