"""Post-run validation of the certified data product tables.

Reimplements the inline BTEQ row-count validation block that used to live in
``orchestration/run_full_pipeline.sh``. For each data product table it verifies
the table exists and is non-empty, logging a row-count summary. Exits non-zero
if any table is missing or empty so the orchestrator can fail fast.

Usage (via spark-submit):
    spark-submit --py-files spark/spark_session.py spark/validate_products.py
"""
from __future__ import annotations

import logging
import os
import sys

from spark_session import build_spark

LOG = logging.getLogger("validate_products")

DATA_PRODUCT_TABLES = (
    "CUSTOMER_SEGMENTS",
    "TRANSACTION_ANALYTICS",
    "CUSTOMER_RISK_SCORES",
    "CUSTOMER_MASTER_PROFILE",
)


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] [validate] %(message)s",
    )

    db_dp = os.environ.get("DB_DP", "DATA_PRODUCTS_DB")
    spark = build_spark("validate")
    failures: list[str] = []
    try:
        LOG.info("Validating data products in database: %s", db_dp)
        for table in sorted(DATA_PRODUCT_TABLES):
            fqtn = f"{db_dp}.{table}"
            if not spark.catalog.tableExists(fqtn):
                LOG.error("MISSING: %s does not exist", fqtn)
                failures.append(table)
                continue
            rows = spark.table(fqtn).count()
            LOG.info("%-24s %12d rows", table, rows)
            if rows == 0:
                LOG.error("EMPTY: %s has zero rows", fqtn)
                failures.append(table)
    except Exception:  # noqa: BLE001 - log full context then fail non-zero
        LOG.exception("Validation failed with an unexpected error.")
        return 1
    finally:
        spark.stop()

    if failures:
        LOG.error("Validation FAILED for: %s", ", ".join(sorted(failures)))
        return 1
    LOG.info("Validation PASSED: all %d data products present and non-empty.",
             len(DATA_PRODUCT_TABLES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
