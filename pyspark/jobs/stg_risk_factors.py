"""Job: stg_risk_factors -- parallel-migration STUB (superseded by its owning ticket).

Loads the committed seed CSV ``data/02_bteq_staging/stg_risk_factors.csv`` into ``{schema_stg}.stg_risk_factors`` so
Ticket 10's end-to-end orchestration test can run before the owning ticket's real
transform lands on the default branch.  Exposes the standard ``run(spark, cfg)``
job contract.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from common.config import Config
from jobs._seed_stub import load_seed

JOB_NAME = "stg_risk_factors"


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    return load_seed(
        spark,
        cfg,
        job_name=JOB_NAME,
        csv_relpath="02_bteq_staging/stg_risk_factors.csv",
        schema=cfg.schema_stg,
        name="stg_risk_factors",
    )
