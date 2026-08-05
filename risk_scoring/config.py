"""Runtime configuration for the PySpark risk-scoring pipeline.

Replaces the SAS ``%sysget`` reads of ``config/pipeline_config.cfg`` and the
hardcoded ``%let`` statements in ``sas/03_sas_risk_scoring.sas``.

Precedence for every value: explicit constructor argument > environment
variable > value parsed from ``config/pipeline_config.cfg`` > built-in default.
Credentials are never sourced from the config file; they come from the
environment (secret store) only.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CFG_PATH = REPO_ROOT / "config" / "pipeline_config.cfg"

_EXPORT_RE = re.compile(r'^\s*export\s+(?P<key>[A-Z_][A-Z0-9_]*)=(?P<value>.*)$')
_DEFAULTED_RE = re.compile(r'^\$\{[A-Z_][A-Z0-9_]*:-(?P<fallback>.*)\}$')


def parse_pipeline_cfg(path: Path = DEFAULT_CFG_PATH) -> dict[str, str]:
    """Parse the ``export KEY=VALUE`` lines of the shell config into a dict.

    Values that are pure command substitutions (``$(date ...)``) are skipped;
    ``${VAR:-fallback}`` collapses to ``fallback``.
    """
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        match = _EXPORT_RE.match(line)
        if not match:
            continue
        raw = match.group("value").split("#", 1)[0].strip().strip('"').strip("'")
        if raw.startswith("$("):
            continue
        defaulted = _DEFAULTED_RE.match(raw)
        if defaulted:
            raw = defaulted.group("fallback")
        values[match.group("key")] = raw
    return values


@dataclass(frozen=True)
class DatabaseRefs:
    """Logical database names, replacing the four ``connect_teradata`` LIBNAMEs."""

    core: str = "CORE_BANKING_DB"
    txn: str = "TXN_PROCESSING_DB"
    staging: str = "ETL_STAGING_DB"
    data_products: str = "DATA_PRODUCTS_DB"


@dataclass(frozen=True)
class TeradataConfig:
    """Connection settings for the Teradata JDBC source/sink.

    ``password`` is resolved from the ``TD_PASSWORD`` environment variable
    (secret store), never from ``config/pipeline_config.cfg`` and never from the
    ``{SAS004}`` placeholders left in the SAS macros.
    """

    server: str = "tdprod.corp.bankdemo.com"
    username: str = "svc_etl_pipeline"
    logmech: str = "LDAP"
    password_env_var: str = "TD_PASSWORD"
    driver: str = "com.teradata.jdbc.TeraDriver"

    @property
    def password(self) -> str:
        password = os.environ.get(self.password_env_var)
        if not password:
            raise RuntimeError(
                f"Teradata password not found in ${self.password_env_var}. "
                "Provide it via the secret store; the SAS {SAS004} placeholders "
                "are not usable credentials."
            )
        return password

    def jdbc_url(self, database: str) -> str:
        return (
            f"jdbc:teradata://{self.server}/DATABASE={database},"
            f"LOGMECH={self.logmech},CHARSET=UTF8"
        )


@dataclass(frozen=True)
class PipelineConfig:
    """Everything ``03_sas_risk_scoring.sas`` read from macro variables or env."""

    model_version: str = "RISK_V4.0"
    risk_score_threshold: int = 700
    job_name: str = "03_RISK_SCORING"

    # Validation gate (SAS: %validate_table(..., min_rows=1000)). Config-driven
    # because the committed sample dataset scores far fewer than 1000 customers.
    min_rows: int = 1000

    # "csv" reads the committed demo extracts under data/; "jdbc" hits Teradata.
    io_backend: str = "csv"
    data_dir: Path = REPO_ROOT / "data"
    output_dir: Path = REPO_ROOT / "output"

    databases: DatabaseRefs = field(default_factory=DatabaseRefs)
    teradata: TeradataConfig = field(default_factory=TeradataConfig)

    @classmethod
    def load(cls, cfg_path: Path = DEFAULT_CFG_PATH, **overrides) -> "PipelineConfig":
        cfg = parse_pipeline_cfg(cfg_path)

        def resolve(key: str, default: str) -> str:
            return os.environ.get(key) or cfg.get(key) or default

        databases = DatabaseRefs(
            core=resolve("DB_CORE", "CORE_BANKING_DB"),
            txn=resolve("DB_TXN", "TXN_PROCESSING_DB"),
            staging=resolve("DB_STG", "ETL_STAGING_DB"),
            data_products=resolve("DB_DP", "DATA_PRODUCTS_DB"),
        )
        teradata = TeradataConfig(
            server=resolve("TD_SERVER", "tdprod.corp.bankdemo.com"),
            username=resolve("TD_USERNAME", "svc_etl_pipeline"),
            logmech=resolve("TD_LOGMECH", "LDAP"),
        )
        values = dict(
            model_version=resolve("MODEL_VERSION", "RISK_V4.0"),
            risk_score_threshold=int(resolve("RISK_SCORE_THRESHOLD", "700")),
            min_rows=int(resolve("RISK_MIN_ROWS", "1000")),
            io_backend=resolve("PIPELINE_IO_BACKEND", "csv").lower(),
            data_dir=Path(resolve("PIPELINE_DATA_DIR", str(REPO_ROOT / "data"))),
            output_dir=Path(resolve("PIPELINE_OUTPUT_DIR", str(REPO_ROOT / "output"))),
            databases=databases,
            teradata=teradata,
        )
        values.update(overrides)
        return cls(**values)
