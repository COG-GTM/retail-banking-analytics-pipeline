# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../src")))

from retail_banking.config import from_widgets


_WIDGETS = {
    "catalog": "",
    "run_date": "",
    "lookback_months": "12",
    "risk_score_threshold": "700",
    "dq_min_rows": "1000",
    "source_path": "",
    "mlflow_experiment": "/Shared/retail_banking",
    "ingest_mode": "batch",
    "skip_silver": "false",
    "skip_gold": "false",
    "dry_run": "false",
}
for _name, _default in _WIDGETS.items():
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:
        pass

cfg = from_widgets(dbutils)
print(f"validation plan: {cfg}; target={cfg.fqn(cfg.gold_schema, 'customer_master_profile')}")
if cfg.dry_run:
    dbutils.notebook.exit("DRY_RUN")

for _table in (
    "customer_segments",
    "transaction_analytics",
    "customer_risk_scores",
    "customer_master_profile",
):
    _fqn = cfg.fqn(cfg.gold_schema, _table)
    print(f"{_fqn}: {spark.table(_fqn).count()} rows")

_log = spark.table(cfg.fqn(cfg.silver_schema, "etl_run_log")).where(
    f"run_date = DATE '{cfg.run_date.isoformat()}'"
)
_log.orderBy("log_ts", ascending=False).show(20, truncate=False)

# COMMAND ----------
