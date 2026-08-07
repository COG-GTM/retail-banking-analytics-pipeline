# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `STG_RISK_FACTORS`
# MAGIC
# MAGIC Port of `bteq/03_stg_risk_factors.bteq`.
# MAGIC
# MAGIC | BTEQ construct | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `WRK_DAILY_BALANCE` / `WRK_PAYMENT_HISTORY` work tables + `DROP` cleanup | in-memory DataFrames (`daily_balance_snapshots`, `payment_history`) |
# MAGIC | `QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID, TRANSACTION_DATE ORDER BY TRANSACTION_TS DESC) = 1` | `Window` + `row_number()` filter |
# MAGIC | `MONTHS_BETWEEN(a, b) (INTEGER)` | `months_between(a, b).cast("int")` |
# MAGIC | `STDDEV_POP` | `stddev_pop` |
# MAGIC | `CURRENT_DATE - 30` | `date_sub(current_date(), 30)` |
# MAGIC | correlated `MERCHANT_NAME NOT IN (SELECT ... WHERE t2.ACCOUNT_ID = t.ACCOUNT_ID ...)` | left join against the distinct prior `(ACCOUNT_ID, MERCHANT_NAME)` set, keeping unmatched rows |
# MAGIC | `QUALIFY ROW_NUMBER() ... ORDER BY REPORT_DATE DESC = 1` (bureau) | `Window` + `row_number()` filter |
# MAGIC
# MAGIC All `COALESCE` defaults are preserved, including `MONTHS_SINCE_LAST_LATE = 999`
# MAGIC and `PAYMENT_ONTIME_PCT = 100.00` for customers with no payment history.

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
from shared.schemas import STG_RISK_FACTORS, conform

JOB_NAME = "03_stg_risk_factors"
TARGET_TABLE = "STG_RISK_FACTORS"

HIGH_RISK_CATEGORIES = ["GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP"]

# COMMAND ----------


def daily_balance_snapshots(transactions: DataFrame, accounts: DataFrame) -> DataFrame:
    """`WRK_DAILY_BALANCE`: last posted running balance per account per day, 3 months."""
    window = Window.partitionBy("ACCOUNT_ID", "TRANSACTION_DATE").orderBy(
        F.col("TRANSACTION_TS").desc()
    )
    t = transactions.alias("t")
    acct = accounts.select("ACCOUNT_ID", "CUSTOMER_ID").alias("acct")
    return (
        t.join(acct, F.col("t.ACCOUNT_ID") == F.col("acct.ACCOUNT_ID"), "inner")
        .where(
            (F.col("t.TRANSACTION_DATE") >= F.add_months(F.current_date(), -3))
            & (F.col("t.STATUS_CODE") == "P")
        )
        .select(
            F.col("acct.CUSTOMER_ID").alias("CUSTOMER_ID"),
            F.col("t.ACCOUNT_ID").alias("ACCOUNT_ID"),
            F.col("t.TRANSACTION_DATE").alias("TRANSACTION_DATE"),
            F.col("t.TRANSACTION_TS").alias("TRANSACTION_TS"),
            F.col("t.RUNNING_BALANCE").alias("EOD_BALANCE"),
        )
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn", "TRANSACTION_TS")
    )


def payment_history(
    transactions: DataFrame, accounts: DataFrame, transaction_types: DataFrame
) -> DataFrame:
    """`WRK_PAYMENT_HISTORY`: on-time / late payments on CREDIT and LOAN accounts."""
    t = transactions.alias("t")
    acct = accounts.alias("acct")
    tt = transaction_types.alias("tt")

    joined = (
        t.join(acct, F.col("t.ACCOUNT_ID") == F.col("acct.ACCOUNT_ID"), "inner")
        .join(tt, F.col("t.TRANSACTION_TYPE_CD") == F.col("tt.TRANSACTION_TYPE_CD"), "inner")
        .where(
            F.col("acct.ACCOUNT_TYPE").isin("CREDIT", "LOAN")
            & (F.col("tt.CATEGORY") == "CREDIT")
            & (F.col("t.STATUS_CODE") == "P")
            & (F.col("t.TRANSACTION_DATE") >= F.add_months(F.current_date(), -24))
        )
    )

    # Due-date proxy: one month after the anniversary of the account open date.
    due_date = F.add_months(
        F.col("acct.OPEN_DATE"),
        F.months_between(F.col("t.TRANSACTION_DATE"), F.col("acct.OPEN_DATE")).cast("int") + 1,
    )
    is_late = F.col("t.TRANSACTION_DATE") > due_date

    return joined.groupBy(
        F.col("acct.CUSTOMER_ID").alias("CUSTOMER_ID"),
        F.col("acct.ACCOUNT_ID").alias("ACCOUNT_ID"),
    ).agg(
        F.count(F.lit(1)).alias("TOTAL_PAYMENTS"),
        F.sum(F.when(~is_late, 1).otherwise(0)).alias("ONTIME_PAYMENTS"),
        F.sum(F.when(is_late, 1).otherwise(0)).alias("LATE_PAYMENTS"),
        F.months_between(
            F.current_date(),
            F.coalesce(
                F.max(F.when(is_late, F.col("t.TRANSACTION_DATE"))),
                F.min(F.col("acct.OPEN_DATE")),
            ),
        )
        .cast("int")
        .alias("MONTHS_SINCE_LAST_LATE"),
    )


def merchant_risk_indicators(transactions: DataFrame, accounts: DataFrame) -> DataFrame:
    """New-merchant / international / high-risk category counts over 6 months."""
    cutoff_30d = F.date_sub(F.current_date(), 30)

    prior_merchants = (
        transactions.where(
            (F.col("TRANSACTION_DATE") < cutoff_30d) & F.col("MERCHANT_NAME").isNotNull()
        )
        .select("ACCOUNT_ID", "MERCHANT_NAME")
        .distinct()
        .withColumn("_SEEN_BEFORE", F.lit(True))
    )

    t = transactions.alias("t")
    acct = accounts.select("ACCOUNT_ID", "CUSTOMER_ID").alias("acct")
    recent = (
        t.join(acct, F.col("t.ACCOUNT_ID") == F.col("acct.ACCOUNT_ID"), "inner")
        .where(
            (F.col("t.TRANSACTION_DATE") >= F.add_months(F.current_date(), -6))
            & (F.col("t.STATUS_CODE") == "P")
        )
        .select(
            F.col("acct.CUSTOMER_ID").alias("CUSTOMER_ID"),
            F.col("t.ACCOUNT_ID").alias("ACCOUNT_ID"),
            F.col("t.TRANSACTION_DATE").alias("TRANSACTION_DATE"),
            F.col("t.MERCHANT_NAME").alias("MERCHANT_NAME"),
            F.col("t.MERCHANT_CATEGORY").alias("MERCHANT_CATEGORY"),
            F.col("t.CHANNEL_CODE").alias("CHANNEL_CODE"),
        )
    )

    flagged = recent.join(prior_merchants, ["ACCOUNT_ID", "MERCHANT_NAME"], "left")

    return flagged.groupBy("CUSTOMER_ID").agg(
        F.countDistinct(
            F.when(
                (F.col("TRANSACTION_DATE") >= cutoff_30d) & F.col("_SEEN_BEFORE").isNull(),
                F.col("MERCHANT_NAME"),
            )
        ).alias("NEW_MERCH_30D"),
        F.sum(F.when(F.col("CHANNEL_CODE") == "INTL", 1).otherwise(0)).alias("INTL_TXN_CNT"),
        F.sum(
            F.when(F.col("MERCHANT_CATEGORY").isin(*HIGH_RISK_CATEGORIES), 1).otherwise(0)
        ).alias("HIGH_RISK_CNT"),
    )


def build_stg_risk_factors(
    customers: DataFrame,
    accounts: DataFrame,
    transactions: DataFrame,
    transaction_types: DataFrame,
    bureau_scores: DataFrame,
) -> DataFrame:
    t = transactions.alias("t")
    acct = accounts.alias("acct")
    tt = transaction_types.alias("tt")

    txn_acct_type = (
        t.join(acct, F.col("t.ACCOUNT_ID") == F.col("acct.ACCOUNT_ID"), "inner")
        .join(tt, F.col("t.TRANSACTION_TYPE_CD") == F.col("tt.TRANSACTION_TYPE_CD"), "inner")
        .select(
            F.col("acct.CUSTOMER_ID").alias("CUSTOMER_ID"),
            F.col("t.TRANSACTION_DATE").alias("TRANSACTION_DATE"),
            F.col("t.AMOUNT").alias("AMOUNT"),
            F.col("t.RUNNING_BALANCE").alias("RUNNING_BALANCE"),
            F.col("t.STATUS_CODE").alias("STATUS_CODE"),
            F.col("tt.CATEGORY").alias("CATEGORY"),
            F.col("tt.DESCRIPTION").alias("DESCRIPTION"),
        )
    )

    posted_12m = txn_acct_type.where(
        (F.col("TRANSACTION_DATE") >= F.add_months(F.current_date(), -12))
        & (F.col("STATUS_CODE") == "P")
    )

    overdraft = posted_12m.groupBy("CUSTOMER_ID").agg(
        F.sum(F.when(F.col("RUNNING_BALANCE") < 0, 1).otherwise(0)).alias("OVERDRAFT_COUNT"),
        F.sum(
            F.when(
                (F.col("CATEGORY") == "FEE") & F.col("DESCRIPTION").like("%NSF%"),
                F.abs(F.col("AMOUNT")),
            ).otherwise(F.lit(0))
        ).alias("NSF_TOTAL"),
    )

    lg_wd = (
        posted_12m.where((F.col("CATEGORY") == "DEBIT") & (F.abs(F.col("AMOUNT")) >= 5000))
        .groupBy("CUSTOMER_ID")
        .agg(
            F.count(F.lit(1)).alias("LARGE_WD_CNT"),
            F.sum(F.abs(F.col("AMOUNT"))).alias("LARGE_WD_AMT"),
        )
    )

    bal = daily_balance_snapshots(transactions, accounts).groupBy("CUSTOMER_ID").agg(
        F.avg(
            F.when(
                F.col("TRANSACTION_DATE") >= F.date_sub(F.current_date(), 30),
                F.col("EOD_BALANCE"),
            )
        ).alias("AVG_BAL_30D"),
        F.avg(
            F.when(
                F.col("TRANSACTION_DATE") >= F.date_sub(F.current_date(), 90),
                F.col("EOD_BALANCE"),
            )
        ).alias("AVG_BAL_90D"),
        F.stddev_pop(F.col("EOD_BALANCE")).alias("BAL_STDDEV"),
    )

    credit = (
        accounts.where((F.col("ACCOUNT_TYPE") == "CREDIT") & (F.col("ACCOUNT_STATUS") == "O"))
        .groupBy("CUSTOMER_ID")
        .agg(
            F.sum(F.coalesce(F.col("CURRENT_BALANCE"), F.lit(0))).alias("TOTAL_CREDIT_BAL"),
            F.sum(F.coalesce(F.col("CREDIT_LIMIT"), F.lit(0))).alias("TOTAL_CREDIT_LIMIT"),
        )
    )

    pmh = payment_history(transactions, accounts, transaction_types).groupBy("CUSTOMER_ID").agg(
        F.sum("TOTAL_PAYMENTS").alias("TOTAL_PAYMENTS"),
        F.sum("ONTIME_PAYMENTS").alias("ONTIME_PAYMENTS"),
        F.sum("LATE_PAYMENTS").alias("LATE_PAYMENTS"),
        F.min("MONTHS_SINCE_LAST_LATE").alias("MONTHS_SINCE_LAST_LATE"),
    )

    bureau_window = Window.partitionBy("CUSTOMER_ID").orderBy(F.col("REPORT_DATE").desc())
    bureau = (
        bureau_scores.withColumn("_rn", F.row_number().over(bureau_window))
        .where(F.col("_rn") == 1)
        .select("CUSTOMER_ID", F.col("EXTERNAL_CREDIT_SCORE").alias("CREDIT_SCORE"))
    )

    vel = (
        txn_acct_type.where(
            (F.col("CATEGORY") == "DEBIT")
            & (F.col("TRANSACTION_DATE") >= F.date_sub(F.current_date(), 30))
            & (F.col("STATUS_CODE") == "P")
        )
        .groupBy("CUSTOMER_ID")
        .agg(
            F.sum(
                F.when(
                    F.col("TRANSACTION_DATE") >= F.date_sub(F.current_date(), 7),
                    F.abs(F.col("AMOUNT")),
                ).otherwise(F.lit(0))
            ).alias("DEBIT_7D"),
            F.sum(
                F.when(
                    F.col("TRANSACTION_DATE") >= F.date_sub(F.current_date(), 30),
                    F.abs(F.col("AMOUNT")),
                ).otherwise(F.lit(0))
            ).alias("DEBIT_30D"),
        )
    )

    merch = merchant_risk_indicators(transactions, accounts)

    c = customers.alias("c")
    joined = (
        c.join(overdraft.alias("overdraft"), ["CUSTOMER_ID"], "left")
        .join(lg_wd.alias("lg_wd"), ["CUSTOMER_ID"], "left")
        .join(bal.alias("bal"), ["CUSTOMER_ID"], "left")
        .join(credit.alias("credit"), ["CUSTOMER_ID"], "left")
        .join(pmh.alias("pmh"), ["CUSTOMER_ID"], "left")
        .join(bureau.alias("bureau"), ["CUSTOMER_ID"], "left")
        .join(vel.alias("vel"), ["CUSTOMER_ID"], "left")
        .join(merch.alias("merch"), ["CUSTOMER_ID"], "left")
        .where(F.col("CUSTOMER_STATUS").isin("A", "I"))
    )

    result = joined.select(
        F.col("CUSTOMER_ID"),
        F.coalesce(F.col("OVERDRAFT_COUNT"), F.lit(0)).alias("ACCOUNT_OVERDRAFT_CNT"),
        F.coalesce(F.col("NSF_TOTAL"), F.lit(0.00)).alias("NSF_FEE_TOTAL"),
        F.coalesce(F.col("LARGE_WD_CNT"), F.lit(0)).alias("LARGE_WITHDRAWAL_CNT"),
        F.coalesce(F.col("LARGE_WD_AMT"), F.lit(0.00)).alias("LARGE_WITHDRAWAL_AMT"),
        F.coalesce(F.col("AVG_BAL_30D"), F.lit(0.00)).alias("AVG_DAILY_BALANCE_30D"),
        F.coalesce(F.col("AVG_BAL_90D"), F.lit(0.00)).alias("AVG_DAILY_BALANCE_90D"),
        F.coalesce(F.col("BAL_STDDEV"), F.lit(0.0000)).alias("BALANCE_VOLATILITY"),
        F.when(
            F.col("TOTAL_CREDIT_LIMIT") > 0,
            (F.col("TOTAL_CREDIT_BAL") / F.col("TOTAL_CREDIT_LIMIT")).cast("decimal(5,4)"),
        )
        .otherwise(F.lit(0.0000).cast("decimal(5,4)"))
        .alias("CREDIT_UTIL_RATIO"),
        F.when(
            F.col("TOTAL_PAYMENTS") > 0,
            (F.col("ONTIME_PAYMENTS") * F.lit(100.0) / F.col("TOTAL_PAYMENTS")).cast(
                "decimal(5,2)"
            ),
        )
        .otherwise(F.lit(100.00).cast("decimal(5,2)"))
        .alias("PAYMENT_ONTIME_PCT"),
        F.coalesce(F.col("LATE_PAYMENTS"), F.lit(0)).alias("PAYMENT_LATE_CNT"),
        F.coalesce(F.col("MONTHS_SINCE_LAST_LATE"), F.lit(999)).alias("MONTHS_SINCE_LAST_LATE"),
        F.coalesce(F.col("CREDIT_SCORE"), F.lit(0)).alias("EXTERNAL_CREDIT_SCORE"),
        F.coalesce(F.col("DEBIT_7D"), F.lit(0.00)).alias("DEBIT_VELOCITY_7D"),
        F.coalesce(F.col("DEBIT_30D"), F.lit(0.00)).alias("DEBIT_VELOCITY_30D"),
        F.coalesce(F.col("NEW_MERCH_30D"), F.lit(0)).alias("NEW_MERCHANT_CNT_30D"),
        F.coalesce(F.col("INTL_TXN_CNT"), F.lit(0)).alias("INTERNATIONAL_TXN_CNT"),
        F.coalesce(F.col("HIGH_RISK_CNT"), F.lit(0)).alias("HIGH_RISK_MERCHANT_CNT"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, STG_RISK_FACTORS)


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_silver", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.silver(TARGET_TABLE)

    with audit.step("FULL_LOAD", f"-> {target}") as ctx:
        df = build_stg_risk_factors(
            spark.table(cfg.bronze("CUSTOMERS")),
            spark.table(cfg.bronze("ACCOUNTS")),
            spark.table(cfg.bronze("TRANSACTIONS")),
            spark.table(cfg.bronze("TRANSACTION_TYPES")),
            spark.table(cfg.bronze("CUSTOMER_BUREAU_SCORES")),
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
        spark.sql(f"OPTIMIZE {target} ZORDER BY (CUSTOMER_ID)")
        ctx.row_count = validate_dataframe(
            spark.table(target),
            target,
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID"],
            min_rows=1,
        )
