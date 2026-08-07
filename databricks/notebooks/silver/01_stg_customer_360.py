# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `STG_CUSTOMER_360`
# MAGIC
# MAGIC Port of `bteq/01_stg_customer_360.bteq`.
# MAGIC
# MAGIC | BTEQ construct | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY EFFECTIVE_DATE DESC) = 1` | `Window.partitionBy("CUSTOMER_ID").orderBy(desc("EFFECTIVE_DATE"))` + `where(row_number == 1)` |
# MAGIC | `MONTHS_BETWEEN` | `months_between` |
# MAGIC | `CURRENT_DATE` | `current_date()` |
# MAGIC | `(CURRENT_DATE - DATE_OF_BIRTH)` (days) | `datediff(current_date(), DATE_OF_BIRTH)` |
# MAGIC | `a || COALESCE(', ' || b, '')` | `concat` (NULL-propagating, as in Teradata) |
# MAGIC | `CREATE TABLE ... WITH DATA` | `write.format("delta").mode("overwrite")` |
# MAGIC | `COLLECT STATISTICS`, `PRIMARY INDEX` | dropped; `OPTIMIZE ... ZORDER BY (CUSTOMER_ID)` |
# MAGIC
# MAGIC Note: `CAST(<double> AS SMALLINT)` truncates in Spark where Teradata rounds.
# MAGIC For `AGE = (CURRENT_DATE - DATE_OF_BIRTH) / 365.25` truncation is the
# MAGIC conventional definition of age, so the Spark behaviour is kept.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shared.audit import AUDIT_TABLE, AuditLogger
from shared.dq import validate_dataframe
from shared.runtime import PipelineConfig, exit_if_skipped, get_param, in_databricks
from shared.schemas import STG_CUSTOMER_360, conform

JOB_NAME = "01_stg_customer_360"
TARGET_TABLE = "STG_CUSTOMER_360"

# COMMAND ----------


def most_recent_home_address(addresses: DataFrame) -> DataFrame:
    """Most recent non-expired HOME address per customer (the BTEQ `QUALIFY` block)."""
    window = Window.partitionBy("CUSTOMER_ID").orderBy(F.col("EFFECTIVE_DATE").desc())
    return (
        addresses.where(
            (F.col("ADDRESS_TYPE") == "HOME")
            & (F.col("EXPIRATION_DATE").isNull() | (F.col("EXPIRATION_DATE") > F.current_date()))
        )
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .select(
            "CUSTOMER_ID", "ADDRESS_LINE_1", "ADDRESS_LINE_2", "CITY", "STATE_CODE", "ZIP_CODE"
        )
    )


def account_portfolio(accounts: DataFrame) -> DataFrame:
    """Per-customer account portfolio metrics (the BTEQ `acct_agg` sub-select)."""
    def has(account_type: str) -> F.Column:
        return F.max(
            F.when(F.col("ACCOUNT_TYPE") == account_type, F.lit("Y")).otherwise(F.lit("N"))
        )

    def credit_amount(column: str) -> F.Column:
        return F.sum(
            F.when(
                F.col("ACCOUNT_TYPE") == "CREDIT", F.coalesce(F.col(column), F.lit(0))
            ).otherwise(F.lit(0))
        )

    return accounts.groupBy("CUSTOMER_ID").agg(
        F.count(F.lit(1)).alias("NUM_ACCOUNTS"),
        F.sum(F.when(F.col("ACCOUNT_STATUS") == "O", 1).otherwise(0)).alias("NUM_ACTIVE_ACCOUNTS"),
        has("CHECKING").alias("HAS_CHECKING"),
        has("SAVINGS").alias("HAS_SAVINGS"),
        has("CREDIT").alias("HAS_CREDIT"),
        has("LOAN").alias("HAS_LOAN"),
        F.sum(F.coalesce(F.col("CURRENT_BALANCE"), F.lit(0))).alias("TOTAL_BALANCE"),
        credit_amount("CREDIT_LIMIT").alias("TOTAL_CREDIT_LIMIT"),
        credit_amount("CURRENT_BALANCE").alias("CREDIT_BALANCE"),
    )


def build_stg_customer_360(
    customers: DataFrame, accounts: DataFrame, addresses: DataFrame
) -> DataFrame:
    addr = most_recent_home_address(addresses).alias("a")
    acct = account_portfolio(accounts).alias("acct")
    cust = customers.alias("c")

    joined = cust.join(addr, F.col("c.CUSTOMER_ID") == F.col("a.CUSTOMER_ID"), "left").join(
        acct, F.col("c.CUSTOMER_ID") == F.col("acct.CUSTOMER_ID"), "left"
    )

    credit_utilization = (
        F.when(
            F.col("acct.TOTAL_CREDIT_LIMIT") > 0,
            (F.col("acct.CREDIT_BALANCE") / F.col("acct.TOTAL_CREDIT_LIMIT") * 100).cast(
                "decimal(5,2)"
            ),
        )
        .otherwise(F.lit(0.00).cast("decimal(5,2)"))
    )

    result = joined.where(F.col("c.CUSTOMER_STATUS").isin("A", "I")).select(
        F.col("c.CUSTOMER_ID").alias("CUSTOMER_ID"),
        F.col("c.FIRST_NAME").alias("FIRST_NAME"),
        F.col("c.LAST_NAME").alias("LAST_NAME"),
        F.col("c.DATE_OF_BIRTH").alias("DATE_OF_BIRTH"),
        (F.datediff(F.current_date(), F.col("c.DATE_OF_BIRTH")) / F.lit(365.25))
        .cast("smallint")
        .alias("AGE"),
        F.col("c.CUSTOMER_SINCE").alias("CUSTOMER_SINCE"),
        F.months_between(F.current_date(), F.col("c.CUSTOMER_SINCE")).cast("int").alias(
            "TENURE_MONTHS"
        ),
        F.col("c.CUSTOMER_STATUS").alias("CUSTOMER_STATUS"),
        F.col("c.SEGMENT_CODE").alias("SEGMENT_CODE"),
        F.col("c.BRANCH_ID").alias("BRANCH_ID"),
        F.concat(
            F.trim(F.col("a.ADDRESS_LINE_1")),
            F.coalesce(F.concat(F.lit(", "), F.trim(F.col("a.ADDRESS_LINE_2"))), F.lit("")),
        ).alias("PRIMARY_ADDRESS"),
        F.col("a.CITY").alias("CITY"),
        F.col("a.STATE_CODE").alias("STATE_CODE"),
        F.col("a.ZIP_CODE").alias("ZIP_CODE"),
        F.col("acct.NUM_ACCOUNTS").alias("NUM_ACCOUNTS"),
        F.col("acct.NUM_ACTIVE_ACCOUNTS").alias("NUM_ACTIVE_ACCOUNTS"),
        F.col("acct.HAS_CHECKING").alias("HAS_CHECKING"),
        F.col("acct.HAS_SAVINGS").alias("HAS_SAVINGS"),
        F.col("acct.HAS_CREDIT").alias("HAS_CREDIT"),
        F.col("acct.HAS_LOAN").alias("HAS_LOAN"),
        F.col("acct.TOTAL_BALANCE").alias("TOTAL_BALANCE"),
        F.col("acct.TOTAL_CREDIT_LIMIT").alias("TOTAL_CREDIT_LIMIT"),
        credit_utilization.alias("CREDIT_UTILIZATION_PCT"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, STG_CUSTOMER_360)


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_silver", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.silver(TARGET_TABLE)

    with audit.step("FULL_LOAD", f"-> {target}") as ctx:
        df = build_stg_customer_360(
            spark.table(cfg.bronze("CUSTOMERS")),
            spark.table(cfg.bronze("ACCOUNTS")),
            spark.table(cfg.bronze("ADDRESSES")),
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
        spark.sql(f"OPTIMIZE {target} ZORDER BY (CUSTOMER_ID)")
        ctx.row_count = validate_dataframe(
            spark.table(target),
            target,
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID", "CUSTOMER_STATUS"],
            min_rows=1,
        )
