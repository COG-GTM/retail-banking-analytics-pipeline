"""Contract stub for synapse/common/config.py (owner: T9). Signatures only — see C4 §4, C7."""
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal, Mapping, Optional, Sequence

Env = Literal["DEV", "UAT", "PROD"]
SqlMode = Literal["synapse", "jdbc", "local"]
AuditSink = Literal["sql", "stdout", "memory"]

SCHEMA_CORE: str  # "CORE_BANKING_DB"
SCHEMA_TXN: str  # "TXN_PROCESSING_DB"
SCHEMA_STG: str  # "ETL_STAGING_DB"
SCHEMA_DP: str  # "DATA_PRODUCTS_DB"
BUSINESS_TIMEZONE: str  # "America/New_York"

@dataclass(frozen=True)
class PipelineConfig:
    env: Env
    run_id: str
    as_of_date: date
    key_vault_name: str
    lookback_months: int = ...
    risk_score_threshold: int = ...
    log_level: str = ...
    sql_mode: SqlMode = ...
    audit_sink: AuditSink = ...
    local_table_dir: Optional[str] = ...
    audit_records: list[dict[str, Any]] = field(default_factory=list)  # used when audit_sink == "memory"
    @property
    def key_vault_url(self) -> str: ...
    @property
    def sql_endpoint(self) -> str: ...  # lazily resolved via keyvault.get_secret("sql-dedicated-endpoint")
    @property
    def sql_database(self) -> str: ...  # "sql-dedicated-database"
    @property
    def storage_account(self) -> str: ...  # "adls-account-name"

def default_as_of_date() -> date: ...  # today in BUSINESS_TIMEZONE
def default_key_vault_name(env: str) -> str: ...  # f"kv-rbap-{env.lower()}"
def load_config(
    env: str,
    run_id: Optional[str] = None,  # None -> uuid4 hex
    as_of_date: Optional[date] = None,  # None -> default_as_of_date()
    key_vault_name: Optional[str] = None,
    overrides: Optional[Mapping[str, Any]] = None,  # explicit CLI/test overrides; RBAP_* env vars applied before these
) -> PipelineConfig: ...
def add_common_args(parser: Any) -> None: ...  # adds --env --run-id --as-of-date --key-vault-name (C7 §1)
def config_from_args(args: Any) -> PipelineConfig: ...
