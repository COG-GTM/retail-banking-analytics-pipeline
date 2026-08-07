# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `STG_CUSTOMER_360`
# MAGIC
# MAGIC Port of `bteq/01_stg_customer_360.bteq`.
# MAGIC
# MAGIC | Teradata construct | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `DROP TABLE` + `CREATE MULTISET TABLE ... WITH DATA PRIMARY INDEX (CUSTOMER_ID)` | Delta `overwrite` with liquid clustering on `CUSTOMER_ID` |
# MAGIC | `COLLECT STATISTICS COLUMN (...)` | `OPTIMIZE` (statistics are maintained by Delta) |
# MAGIC | `QUALIFY ROW_NUMBER() OVER (...) = 1` | identical — Databricks SQL supports `QUALIFY` |
# MAGIC | `CURRENT_DATE - DATE_OF_BIRTH` (day difference) | `datediff(run_date, DATE_OF_BIRTH)` |
# MAGIC | `MONTHS_BETWEEN(CURRENT_DATE, CUSTOMER_SINCE)` | `months_between(run_date, CUSTOMER_SINCE)` |
# MAGIC | `CAST(<fractional> AS SMALLINT/INTEGER)` | identical — both engines truncate the fractional part on a decimal-to-integer cast |
# MAGIC | `CURRENT_TIMESTAMP(6)` | `current_timestamp()` |
# MAGIC | `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99` + `INSERT INTO ETL_RUN_LOG` | `shared.validation` + `shared.audit` |
# MAGIC
# MAGIC `CURRENT_DATE` is replaced by the `run_date` job parameter so a run is
# MAGIC reproducible and can be backfilled for an as-of date.

# COMMAND ----------

from __future__ import annotations

import os
import sys


def _bootstrap() -> None:
    here = os.path.dirname(os.path.abspath(globals().get("__file__", os.path.join(os.getcwd(), "nb.py"))))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)


_bootstrap()

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from shared import io, schemas  # noqa: E402
from shared.audit import ensure_run_log, step  # noqa: E402
from shared.config import PipelineConfig, exit_if_skipped  # noqa: E402
from shared.logging_utils import get_logger, log_event  # noqa: E402
from shared.validation import validate_and_log  # noqa: E402

JOB_NAME = "01_stg_customer_360"
TARGET_TABLE = "STG_CUSTOMER_360"

# COMMAND ----------


def build_stg_customer_360(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Denormalized customer 360: customer + current HOME address + accounts."""
    run_date = cfg.run_date_literal
    return spark.sql(
        f"""
        WITH current_address AS (
            /* Most recent non-expired HOME address, one row per customer */
            SELECT
                CUSTOMER_ID,
                ADDRESS_LINE_1,
                ADDRESS_LINE_2,
                CITY,
                STATE_CODE,
                ZIP_CODE
            FROM {cfg.bronze('ADDRESSES')}
            WHERE ADDRESS_TYPE = 'HOME'
              AND (EXPIRATION_DATE IS NULL OR EXPIRATION_DATE > {run_date})
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CUSTOMER_ID
                ORDER BY EFFECTIVE_DATE DESC
            ) = 1
        ),
        account_agg AS (
            SELECT
                CUSTOMER_ID,
                COUNT(*)                                                        AS NUM_ACCOUNTS,
                SUM(CASE WHEN ACCOUNT_STATUS = 'O' THEN 1 ELSE 0 END)           AS NUM_ACTIVE_ACCOUNTS,
                MAX(CASE WHEN ACCOUNT_TYPE = 'CHECKING' THEN 'Y' ELSE 'N' END)  AS HAS_CHECKING,
                MAX(CASE WHEN ACCOUNT_TYPE = 'SAVINGS'  THEN 'Y' ELSE 'N' END)  AS HAS_SAVINGS,
                MAX(CASE WHEN ACCOUNT_TYPE = 'CREDIT'   THEN 'Y' ELSE 'N' END)  AS HAS_CREDIT,
                MAX(CASE WHEN ACCOUNT_TYPE = 'LOAN'     THEN 'Y' ELSE 'N' END)  AS HAS_LOAN,
                SUM(COALESCE(CURRENT_BALANCE, 0))                               AS TOTAL_BALANCE,
                SUM(CASE WHEN ACCOUNT_TYPE = 'CREDIT'
                         THEN COALESCE(CREDIT_LIMIT, 0)
                         ELSE 0 END)                                            AS TOTAL_CREDIT_LIMIT,
                SUM(CASE WHEN ACCOUNT_TYPE = 'CREDIT'
                         THEN COALESCE(CURRENT_BALANCE, 0)
                         ELSE 0 END)                                            AS CREDIT_BALANCE
            FROM {cfg.bronze('ACCOUNTS')}
            GROUP BY CUSTOMER_ID
        )
        SELECT
            c.CUSTOMER_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.DATE_OF_BIRTH,
            /* Derive age from date of birth */
            CAST(datediff({run_date}, c.DATE_OF_BIRTH) / 365.25 AS SMALLINT)        AS AGE,
            c.CUSTOMER_SINCE,
            /* Tenure in months since account opening */
            CAST(months_between({run_date}, c.CUSTOMER_SINCE) AS INT)               AS TENURE_MONTHS,
            c.CUSTOMER_STATUS,
            c.SEGMENT_CODE,
            c.BRANCH_ID,
            /* Concatenated primary address */
            TRIM(a.ADDRESS_LINE_1) || COALESCE(', ' || TRIM(a.ADDRESS_LINE_2), '')  AS PRIMARY_ADDRESS,
            a.CITY,
            a.STATE_CODE,
            a.ZIP_CODE,
            acct_agg.NUM_ACCOUNTS,
            acct_agg.NUM_ACTIVE_ACCOUNTS,
            acct_agg.HAS_CHECKING,
            acct_agg.HAS_SAVINGS,
            acct_agg.HAS_CREDIT,
            acct_agg.HAS_LOAN,
            acct_agg.TOTAL_BALANCE,
            acct_agg.TOTAL_CREDIT_LIMIT,
            /* Credit utilization = total credit balance / total credit limit */
            CASE
                WHEN acct_agg.TOTAL_CREDIT_LIMIT > 0
                THEN CAST(acct_agg.CREDIT_BALANCE / acct_agg.TOTAL_CREDIT_LIMIT * 100 AS DECIMAL(5,2))
                ELSE 0.00
            END                                                                     AS CREDIT_UTILIZATION_PCT,
            current_timestamp()                                                     AS LOAD_TS
        FROM {cfg.bronze('CUSTOMERS')} c
        LEFT JOIN current_address a  ON c.CUSTOMER_ID = a.CUSTOMER_ID
        LEFT JOIN account_agg acct_agg ON c.CUSTOMER_ID = acct_agg.CUSTOMER_ID
        WHERE c.CUSTOMER_STATUS IN ('A', 'I')   /* Exclude closed customers */
        """
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    ensure_run_log(spark, cfg)
    target = cfg.silver(TARGET_TABLE)

    with step(spark, cfg, JOB_NAME, "FULL_LOAD") as ctx:
        df = schemas.conform(
            build_stg_customer_360(spark, cfg), schemas.SILVER_SCHEMAS[TARGET_TABLE]
        )
        ctx["row_count"] = io.write_table(spark, cfg, df, target, merge_keys=["CUSTOMER_ID"])
        rows = ctx["row_count"]

    validate_and_log(
        spark,
        cfg,
        JOB_NAME,
        target,
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID", "LAST_NAME"],
    )
    return rows


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig.from_widgets(spark)
    logger = get_logger()
    log_event(logger, "job_start", run_id=cfg.run_id, job=JOB_NAME, config=cfg.describe())

    if not exit_if_skipped(cfg, "silver", spark):
        log_event(
            logger,
            "job_complete",
            run_id=cfg.run_id,
            job=JOB_NAME,
            table=TARGET_TABLE,
            row_count=run(spark, cfg),
        )
