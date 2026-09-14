import datetime as dt
import os
from dataclasses import dataclass, field


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
        run_date=run_date,
        lookback_months=int(values.get("lookback_months") or 12),
        risk_score_threshold=int(values.get("risk_score_threshold") or 700),
        dq_min_rows=int(values.get("dq_min_rows") or 1000),
        mlflow_enabled=_parse_bool(values.get("mlflow_enabled"), True),
        mlflow_experiment=values.get("mlflow_experiment") or "/Shared/retail_banking",
        source_path=values.get("source_path") or None,
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
    }
    return _config(values)
