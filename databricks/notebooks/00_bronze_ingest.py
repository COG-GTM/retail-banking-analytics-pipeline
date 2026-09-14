# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../src")))

from retail_banking.bronze.ingest import ingest_autoloader, ingest_batch
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
print(f"bronze plan: {cfg}; target={cfg.fqn(cfg.bronze_core_schema, 'customers')}")
if cfg.dry_run:
    dbutils.notebook.exit("DRY_RUN")
if not cfg.source_path:
    raise ValueError("source_path is required for bronze ingestion")
if cfg.ingest_mode == "batch":
    ingest_batch(spark, cfg, cfg.source_path)
elif cfg.ingest_mode == "autoloader":
    ingest_autoloader(spark, cfg, cfg.source_path, "/tmp/retail-banking-autoloader")
else:
    raise ValueError(f"Unsupported ingest_mode: {cfg.ingest_mode}")

# COMMAND ----------
