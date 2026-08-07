"""Runtime helpers: parameter resolution, table naming and skip flags.

Replaces ``config/pipeline_config.cfg``. Every value that used to be an exported
shell variable is now a notebook widget, which Databricks Workflows overrides
with the matching job parameter.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULTS = {
    "catalog": "retail_banking",
    "bronze_schema": "core_banking",
    "silver_schema": "etl_staging",
    "gold_schema": "data_products",
    "ops_schema": "_ops",
    "source_data_path": "/Volumes/retail_banking/core_banking/landing",
    "lookback_months": "12",
    "risk_score_threshold": "700",
    "skip_silver": "false",
    "skip_gold": "false",
}


def in_databricks() -> bool:
    """True when executing on a Databricks cluster (as opposed to local tests)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _dbutils():
    import IPython

    return IPython.get_ipython().user_ns["dbutils"]


def get_param(name: str, default: str | None = None) -> str:
    """Read a notebook widget, falling back to :data:`DEFAULTS`.

    The widget is created on demand so a notebook can be run interactively
    without any prior setup.
    """
    fallback = DEFAULTS.get(name, "") if default is None else default
    if not in_databricks():
        return os.environ.get(name.upper(), fallback)
    dbutils = _dbutils()
    dbutils.widgets.text(name, fallback)
    value = dbutils.widgets.get(name)
    return value if value != "" else fallback


def get_bool_param(name: str, default: str | None = None) -> bool:
    return get_param(name, default).strip().lower() in ("true", "1", "yes", "y")


def get_int_param(name: str, default: str | None = None) -> int:
    return int(get_param(name, default))


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved catalog/schema layout for one pipeline run."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    ops_schema: str

    @classmethod
    def from_widgets(cls) -> PipelineConfig:
        return cls(
            catalog=get_param("catalog"),
            bronze_schema=get_param("bronze_schema"),
            silver_schema=get_param("silver_schema"),
            gold_schema=get_param("gold_schema"),
            ops_schema=get_param("ops_schema"),
        )

    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    def ops(self, table: str) -> str:
        return f"{self.catalog}.{self.ops_schema}.{table}"


def exit_if_skipped(flag: str, task: str) -> bool:
    """Mirror the ``--skip-bteq`` / ``--skip-sas`` switches of the shell orchestrator.

    Returns True when the caller should stop; on Databricks the notebook is
    exited immediately so downstream tasks still run against existing tables.
    """
    if not get_bool_param(flag):
        return False
    message = f"SKIPPED: {task} (job parameter {flag}=true)"
    print(message)
    if in_databricks():
        _dbutils().notebook.exit(message)
    return True
