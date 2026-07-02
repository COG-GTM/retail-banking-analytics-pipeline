#!/usr/bin/env python3
"""PySpark staging pipeline driver.

Replaces the former BTEQ orchestrator ``bteq/run_bteq_pipeline.sh``. Runs the
three staging transforms in dependency order with fail-fast error handling,
translating the BTEQ dot-command control flow:

  * ``.LOGON`` / ``.LOGOFF``            -> SparkSession lifecycle.
  * ``.IF ERRORCODE <> 0 THEN .EXIT``   -> non-zero process exit on any job error.
  * ``.IF ACTIVITYCOUNT = 0 THEN .EXIT 99`` -> zero-row guard (exit code 99).
  * ``.QUIT`` / ``.EXIT 0``             -> ``sys.exit`` with the aggregate status.

Run from the repo root:
    python -m staging.run_staging_pipeline
    # or:  python staging/run_staging_pipeline.py
Configuration is environment-driven (see :mod:`staging.config`).
"""
from __future__ import annotations

import os
import sys

# Allow execution both as a module (`-m`) and as a plain script.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from staging import stg_customer_360, stg_risk_factors, stg_txn_summary  # noqa: E402
from staging.config import load_config  # noqa: E402
from staging.spark_utils import build_spark_session, get_logger  # noqa: E402

# Order matters: risk factors depends on the same sources, and the sequence
# mirrors the original 01 -> 02 -> 03 BTEQ schedule.
STAGES = [stg_customer_360, stg_txn_summary, stg_risk_factors]

ZERO_ROW_EXIT_CODE = 99


def main() -> int:
    cfg = load_config()
    logger = get_logger(cfg, "staging.pipeline")
    logger.info(
        "pipeline start",
        status="START",
        run_id=cfg.run_id,
        as_of_date=cfg.as_of_date_iso,
        lookback_months=cfg.lookback_months,
        output_format=cfg.output_format,
        output_dir=cfg.output_dir,
    )

    spark = build_spark_session(cfg)
    spark.sparkContext.setLogLevel("WARN")
    try:
        for stage in STAGES:
            name = stage.TABLE_NAME
            try:
                rows = stage.run(spark, cfg)
            except Exception:  # fail fast, mirroring .IF ERRORCODE <> 0 THEN .EXIT
                logger.error("stage failed", step=name, status="FAILED", exc_info=True)
                return 1
            if rows == 0:  # mirrors .IF ACTIVITYCOUNT = 0 THEN .EXIT 99
                logger.error("stage produced zero rows", step=name, status="FAILED", row_count=0)
                return ZERO_ROW_EXIT_CODE
        logger.info("pipeline complete", status="SUCCESS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
