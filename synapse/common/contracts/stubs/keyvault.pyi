"""Contract stub for synapse/common/keyvault.py (owner: T9). See C4 §2.

Named keyvault (not secrets) to avoid shadowing the stdlib `secrets` module.
"""
from typing import Optional
from common.config import PipelineConfig

SECRET_SQL_ENDPOINT: str  # "sql-dedicated-endpoint"
SECRET_SQL_DATABASE: str  # "sql-dedicated-database"
SECRET_ADLS_ACCOUNT: str  # "adls-account-name"
SECRET_ADLS_LANDING_CONTAINER: str  # "adls-landing-container"
SECRET_ADLS_ARTIFACTS_CONTAINER: str  # "adls-artifacts-container"
KEYVAULT_LINKED_SERVICE: str  # "ls_keyvault"

def env_var_name(secret_name: str) -> str: ...  # "sql-dedicated-endpoint" -> "RBAP_SECRET_SQL_DEDICATED_ENDPOINT"
def is_synapse_runtime() -> bool: ...
def get_secret(cfg: PipelineConfig, name: str, default: Optional[str] = None) -> str: ...
def clear_cache() -> None: ...
