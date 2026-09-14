import datetime as dt
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class RunConfig:
    catalog: str | None
    bronze_core_schema: str = "core_banking"
    bronze_txn_schema: str = "txn_processing"
    silver_schema: str = "etl_staging"
    gold_schema: str = "data_products"
    run_date: dt.date = field(default_factory=dt.date.today)
    lookback_months: int = 12
    risk_score_threshold: int = 700
    dq_min_rows: int = 1000
    mlflow_enabled: bool = True
    mlflow_experiment: str = "/Shared/retail_banking"
    source_path: str | None = None
    ingest_mode: str = "batch"
    skip_silver: bool = False
    skip_gold: bool = False
    dry_run: bool = False

    def fqn(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}" if self.catalog else f"{schema}.{table}"


def _parse_bool(value: str | bool | None, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _config(values: dict[str, str | None]) -> RunConfig:
    today = dt.datetime.now(dt.timezone.utc).date()
    run_date = dt.date.fromisoformat(values["run_date"]) if values.get("run_date") else today
    return RunConfig(
        catalog=values.get("catalog") or None,
        bronze_core_schema=values.get("bronze_core_schema") or "core_banking",
        bronze_txn_schema=values.get("bronze_txn_schema") or "txn_processing",
        silver_schema=values.get("silver_schema") or "etl_staging",
        gold_schema=values.get("gold_schema") or "data_products",
        run_date=run_date,
        lookback_months=int(values.get("lookback_months") or 12),
        risk_score_threshold=int(values.get("risk_score_threshold") or 700),
        dq_min_rows=int(values.get("dq_min_rows") or 1000),
        mlflow_enabled=_parse_bool(values.get("mlflow_enabled"), True),
        mlflow_experiment=values.get("mlflow_experiment") or "/Shared/retail_banking",
        source_path=values.get("source_path") or None,
        ingest_mode=values.get("ingest_mode") or "batch",
        skip_silver=_parse_bool(values.get("skip_silver"), False),
        skip_gold=_parse_bool(values.get("skip_gold"), False),
        dry_run=_parse_bool(values.get("dry_run"), False),
    )


def from_widgets(dbutils) -> RunConfig:
    names = (
        "catalog",
        "run_date",
        "lookback_months",
        "risk_score_threshold",
        "dq_min_rows",
        "source_path",
        "mlflow_experiment",
        "mlflow_enabled",
        "ingest_mode",
        "skip_silver",
        "skip_gold",
        "dry_run",
    )
    values = {}
    for name in names:
        try:
            values[name] = dbutils.widgets.get(name)
        except Exception:  # noqa: BLE001
            values[name] = None
    return _config(values)


def from_env() -> RunConfig:
    values = {
        "catalog": os.getenv("CATALOG"),
        "run_date": os.getenv("RUN_DATE"),
        "lookback_months": os.getenv("LOOKBACK_MONTHS"),
        "risk_score_threshold": os.getenv("RISK_SCORE_THRESHOLD"),
        "dq_min_rows": os.getenv("DQ_MIN_ROWS"),
        "source_path": os.getenv("SOURCE_PATH"),
        "mlflow_experiment": os.getenv("MLFLOW_EXPERIMENT"),
        "mlflow_enabled": os.getenv("MLFLOW_ENABLED"),
        "ingest_mode": os.getenv("INGEST_MODE"),
        "skip_silver": os.getenv("SKIP_SILVER"),
        "skip_gold": os.getenv("SKIP_GOLD"),
        "dry_run": os.getenv("DRY_RUN"),
    }
    return _config(values)


def from_conf(path: str | os.PathLike[str], env: str) -> RunConfig:
    document = yaml.safe_load(Path(path).read_text()) or {}
    values = document.get(env, document)
    if not isinstance(values, dict):
        raise TypeError(f"Configuration for environment {env!r} must be a mapping")
    return _config(
        {str(key).lower(): None if value is None else str(value) for key, value in values.items()}
    )
