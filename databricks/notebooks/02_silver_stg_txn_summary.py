# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../src")))

from retail_banking.config import from_widgets
from retail_banking.silver import stg_txn_summary


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
print(f"silver plan: {cfg.fqn(cfg.silver_schema, 'stg_txn_summary')}")
if cfg.dry_run:
    dbutils.notebook.exit("DRY_RUN")
if cfg.skip_silver:
    dbutils.notebook.exit("SKIPPED")
stg_txn_summary.run(spark, cfg)

# COMMAND ----------
