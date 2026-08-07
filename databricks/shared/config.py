"""Pipeline configuration.

Replaces ``config/pipeline_config.cfg`` (a shell file that exported Teradata
hostnames, SAS paths and run parameters) with Databricks **job parameters**
resolved through notebook widgets.

Nothing in here holds a credential: table access is governed by Unity Catalog
and the only secret material (an optional JDBC password used when ingesting
straight from the legacy Teradata system) is read from a Databricks secret
scope at run time via :func:`shared.secrets.get_secret`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from shared.logging_utils import get_logger, log_event

# Parameter name -> default value. Every entry becomes a notebook widget and a
# job parameter, and can also be overridden with the RBA_<NAME> env var.
PARAM_DEFAULTS: dict[str, str] = {
    # Unity Catalog placement (replaces CORE_BANKING_DB / TXN_PROCESSING_DB /
    # ETL_STAGING_DB / DATA_PRODUCTS_DB)
    "catalog": "retail_banking",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold",
    "ops_schema": "ops",
    # Run parameters (replaces RUN_DATE / LOOKBACK_MONTHS / RISK_SCORE_THRESHOLD)
    "run_date": "",  # empty -> current_date; set explicitly for backfills
    "lookback_months": "12",
    "risk_score_threshold": "700",
    # Bronze ingestion
    "source_format": "csv",  # csv | jdbc
    "source_data_path": "",  # empty -> /Volumes/<catalog>/<bronze>/landing/01_source_tables
    "secret_scope": "retail-banking-analytics",
    # Post-run validation: legacy pipeline CSVs to compare against (optional)
    "reference_data_path": "",
    # Orchestration switches (replace run_full_pipeline.sh --skip-bteq/--skip-sas)
    "skip_bronze": "false",
    "skip_silver": "false",  # the BTEQ staging layer
    "skip_gold": "false",  # the SAS analytics layer
    "skip_bteq": "false",  # legacy alias for skip_silver
    "skip_sas": "false",  # legacy alias for skip_gold
    # Storage behaviour
    "table_format": "delta",
    "optimize_tables": "true",
    "write_mode": "overwrite",  # overwrite | merge
    "min_rows": "1",  # %validate_table min_rows (SAS used 1000 on prod volumes)
}

_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in _TRUE


def _dbutils(spark: Any = None) -> Any:
    """Return ``dbutils`` when running on Databricks, otherwise ``None``."""
    try:  # notebook / job context
        import IPython

        shell = IPython.get_ipython()
        if shell is not None and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:  # pragma: no cover - IPython absent locally
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        return DBUtils(spark)
    except Exception:  # pragma: no cover - open-source Spark
        return None


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved configuration for a single pipeline run."""

    catalog: str = PARAM_DEFAULTS["catalog"]
    bronze_schema: str = PARAM_DEFAULTS["bronze_schema"]
    silver_schema: str = PARAM_DEFAULTS["silver_schema"]
    gold_schema: str = PARAM_DEFAULTS["gold_schema"]
    ops_schema: str = PARAM_DEFAULTS["ops_schema"]
    run_date: date = field(default_factory=date.today)
    lookback_months: int = 12
    risk_score_threshold: int = 700
    source_format: str = "csv"
    source_data_path: str = ""
    secret_scope: str = PARAM_DEFAULTS["secret_scope"]
    reference_data_path: str = ""
    skip_bronze: bool = False
    skip_silver: bool = False
    skip_gold: bool = False
    table_format: str = "delta"
    optimize_tables: bool = True
    write_mode: str = "overwrite"
    min_rows: int = 1
    run_id: str = ""

    # -- table name helpers -------------------------------------------------
    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    def ops(self, table: str) -> str:
        return f"{self.catalog}.{self.ops_schema}.{table}"

    @property
    def schemas(self) -> tuple[str, ...]:
        return (self.bronze_schema, self.silver_schema, self.gold_schema, self.ops_schema)

    @property
    def landing_path(self) -> str:
        """Volume path holding the source CSVs produced by ``export_data.py``."""
        if self.source_data_path:
            return self.source_data_path.rstrip("/")
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/landing/01_source_tables"

    @property
    def run_date_literal(self) -> str:
        """``DATE 'yyyy-MM-dd'`` literal used in place of Teradata CURRENT_DATE."""
        return f"DATE '{self.run_date.isoformat()}'"

    @property
    def reporting_period(self) -> str:
        """``YYYY-MM`` — the SAS ``&REPORTING_PERIOD`` macro variable."""
        return self.run_date.strftime("%Y-%m")

    def describe(self) -> str:
        return (
            f"catalog={self.catalog} schemas=({', '.join(self.schemas)}) "
            f"run_date={self.run_date.isoformat()} lookback_months={self.lookback_months} "
            f"risk_score_threshold={self.risk_score_threshold} table_format={self.table_format} "
            f"write_mode={self.write_mode} run_id={self.run_id}"
        )

    # -- construction -------------------------------------------------------
    @classmethod
    def from_widgets(cls, spark: Any = None) -> PipelineConfig:
        """Build the config from notebook widgets, env vars, then defaults."""
        dbu = _dbutils(spark)
        values: dict[str, str] = {}
        for name, default in PARAM_DEFAULTS.items():
            value = default
            if dbu is not None:
                try:
                    dbu.widgets.text(name, default)
                    value = dbu.widgets.get(name)
                except Exception:  # widget API unavailable (e.g. DLT context)
                    value = default
            value = os.environ.get(f"RBA_{name.upper()}", value)
            values[name] = value

        run_id = ""
        if dbu is not None:
            try:
                ctx = dbu.notebook.entry_point.getDbutils().notebook().getContext()
                run_id = ctx.currentRunId().toString()
            except Exception:
                run_id = ""
        run_id = run_id or os.environ.get("DATABRICKS_RUN_ID", "") or datetime.now().strftime(
            "local_%Y%m%d_%H%M%S"
        )

        run_date_raw = values["run_date"].strip()
        run_date = date.fromisoformat(run_date_raw) if run_date_raw else date.today()

        return cls(
            catalog=values["catalog"],
            bronze_schema=values["bronze_schema"],
            silver_schema=values["silver_schema"],
            gold_schema=values["gold_schema"],
            ops_schema=values["ops_schema"],
            run_date=run_date,
            lookback_months=int(values["lookback_months"]),
            risk_score_threshold=int(values["risk_score_threshold"]),
            source_format=values["source_format"].strip().lower(),
            source_data_path=values["source_data_path"].strip(),
            secret_scope=values["secret_scope"],
            reference_data_path=values["reference_data_path"].strip(),
            skip_bronze=_as_bool(values["skip_bronze"]),
            skip_silver=_as_bool(values["skip_silver"]) or _as_bool(values["skip_bteq"]),
            skip_gold=_as_bool(values["skip_gold"]) or _as_bool(values["skip_sas"]),
            table_format=values["table_format"].strip().lower(),
            optimize_tables=_as_bool(values["optimize_tables"]),
            write_mode=values["write_mode"].strip().lower(),
            min_rows=int(values["min_rows"]),
            run_id=run_id,
        )


def exit_if_skipped(cfg: PipelineConfig, layer: str, spark: Any = None) -> bool:
    """Honour the ``--skip-bteq`` / ``--skip-sas`` equivalents.

    Returns ``True`` when the caller should stop. On Databricks the notebook is
    exited so the workflow marks the task as succeeded-and-skipped.
    """
    skipped = {"bronze": cfg.skip_bronze, "silver": cfg.skip_silver, "gold": cfg.skip_gold}[layer]
    if not skipped:
        return False
    message = f"SKIPPED: {layer} layer disabled by job parameter skip_{layer}=true"
    log_event(get_logger(), "layer_skipped", run_id=cfg.run_id, layer=layer, message=message)
    dbu = _dbutils(spark)
    if dbu is not None:
        dbu.notebook.exit(message)
    return True
