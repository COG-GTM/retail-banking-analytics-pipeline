"""Job: risk_scoring -- parallel-migration STUB (superseded by its owning ticket).

Loads the committed seed CSV ``data/03_sas_data_products/customer_risk_scores.csv`` into ``{schema_dp}.customer_risk_scores`` so
Ticket 10's end-to-end orchestration test can run before the owning ticket's real
transform lands on the default branch.  Exposes the standard ``run(spark, cfg)``
job contract.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from common.config import Config
from jobs._seed_stub import load_seed

JOB_NAME = "risk_scoring"


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    return load_seed(
        spark,
        cfg,
        job_name=JOB_NAME,
        csv_relpath="03_sas_data_products/customer_risk_scores.csv",
        schema=cfg.schema_dp,
        name="customer_risk_scores",
    )
