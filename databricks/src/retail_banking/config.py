from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


def _parse_run_date(value: str | date | None) -> date:
    if value is None or value == "":
        return date.today()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


@dataclass
class PipelineConfig:
    catalog: str
    bronze_schema: str = "core_banking"
    txn_schema: str = "txn_processing"
    silver_schema: str = "etl_staging"
    gold_schema: str = "data_products"
    lookback_months: int = 12
    risk_score_threshold: int = 700
    min_gold_rows: int = 1000
    run_date: date = field(default_factory=date.today)

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def bronze_table(self, name: str) -> str:
        return self.table(self.bronze_schema, name)

    def txn_table(self, name: str) -> str:
        return self.table(self.txn_schema, name)

    def silver_table(self, name: str) -> str:
        return self.table(self.silver_schema, name)

    def gold_table(self, name: str) -> str:
        return self.table(self.gold_schema, name)

    @property
    def audit_table(self) -> str:
        return self.table(self.silver_schema, "pipeline_audit")

    @property
    def landing_volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/landing"

    @property
    def parity_volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.gold_schema}/parity_baseline"

    @classmethod
    def from_widgets(cls, dbutils) -> "PipelineConfig":
        def w(name: str, default: str = "") -> str:
            try:
                return dbutils.widgets.get(name) or default
            except Exception:  # noqa: BLE001 - widget may not exist at all
                return default

        return cls(
            catalog=w("catalog", "retail_banking_dev"),
            lookback_months=int(w("lookback_months", "12")),
            risk_score_threshold=int(w("risk_score_threshold", "700")),
            min_gold_rows=int(w("min_gold_rows", "1000")),
            run_date=_parse_run_date(w("run_date", "")),
        )

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            catalog=os.environ.get("RB_CATALOG", "retail_banking_dev"),
            lookback_months=int(os.environ.get("RB_LOOKBACK_MONTHS", "12")),
            risk_score_threshold=int(os.environ.get("RB_RISK_SCORE_THRESHOLD", "700")),
            min_gold_rows=int(os.environ.get("RB_MIN_GOLD_ROWS", "1000")),
            run_date=_parse_run_date(os.environ.get("RB_RUN_DATE", "")),
        )
