"""DDL bootstrap -- parallel-migration STUB (superseded by Ticket 1).

Ticket 1 owns the canonical Unity Catalog / Delta DDL and this bootstrap.  This
minimal stub ensures the local Delta warehouse root exists and the audit table
(``etl_staging.etl_run_log``) is created, so the orchestration DAG has a valid
first step (``create_delta_tables``) before the downstream jobs run.  Exposes the
standard ``run(spark, cfg)`` contract.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from pyspark.sql import SparkSession

from common.audit import init_audit, log_step
from common.config import Config

JOB_NAME = "create_delta_tables"


def run(spark: SparkSession, cfg: Config) -> None:
    Path(cfg.warehouse_dir).mkdir(parents=True, exist_ok=True)
    init_audit(spark, cfg)
    run_id = str(uuid.uuid4())
    log_step(spark, cfg, run_id, JOB_NAME, "bootstrap", "SUCCESS",
             message="[stub] Delta warehouse root and audit table ready")
