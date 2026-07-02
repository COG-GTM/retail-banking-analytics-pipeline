"""Run the full PySpark analytics pipeline (01 -> 04).

Replaces ``sas/run_sas_pipeline.sh``'s in-order execution of the four SAS
programs. Each job reads staging datasets (produced by the BTEQ layer) and
writes the certified data-product datasets.
"""
from __future__ import annotations

import sys

from ..config import PipelineConfig
from ..logging_utils import get_logger
from . import customer_segments, data_products, risk_scoring, txn_analytics

JOBS = [
    ("01_customer_segments", customer_segments.run),
    ("02_txn_analytics", txn_analytics.run),
    ("03_risk_scoring", risk_scoring.run),
    ("04_data_products", data_products.run),
]


def main() -> int:
    config = PipelineConfig.from_env()
    log = get_logger(config.run_id)
    log.info("=========================================")
    log.info("Spark Pipeline Start - Run: %s", config.run_id)
    log.info("=========================================")

    for name, job in JOBS:
        log.info("START: %s", name)
        path = job(config)
        log.info("SUCCESS: %s -> %s", name, path)

    log.info("=========================================")
    log.info("Spark Pipeline Complete")
    log.info("=========================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
