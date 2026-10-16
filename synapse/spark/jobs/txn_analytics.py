"""Port of sas/02_sas_txn_analytics.sas to PySpark.

Legacy -> Spark mapping:
  STGDB.STG_TXN_SUMMARY            -> stg.STG_TXN_SUMMARY (pipeline_utils.read_table)
  PROC SQL customer-level rollup   -> groupBy(CUSTOMER_ID) aggregations
  PROC RANK groups=100             -> proc_rank_groups()
  PROC MEANS median/qrange         -> qntldef5_quantile() (SAS QNTLDEF=5)
  DATA step trend/anomaly flags    -> column expressions
  %validate_table / %log_step      -> pipeline_utils validate_table / log_step
  PROC APPEND -> DPDB              -> write_table(append, pre_delete_where)
Target table: dp.TRANSACTION_ANALYTICS (ddl/02_data_product_tables.sql).
"""

import sys
from datetime import date
from decimal import Decimal
from fractions import Fraction

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

JOB_NAME = "02_sas_txn_analytics"
STEP_NAME = "02_TXN_ANALYTICS"
MODEL_VERSION = "TXN_V2.1"
ANOMALY_IQR_MULTIPLIER = 3
SPEND_PERCENTILE_GROUPS = 100
VALIDATION_KEY_COLS = ["CUSTOMER_ID"]
VALIDATION_NOT_NULL_COLS = ["CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"]
VALIDATION_MIN_ROWS = 1000  # faithful to the SAS %validate_table min_rows=1000

OUTPUT_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType(), False),
        StructField("REPORTING_PERIOD", StringType(), True),
        StructField("TOTAL_ACCOUNTS", ShortType(), True),
        StructField("ACTIVE_ACCOUNTS", ShortType(), True),
        StructField("TOTAL_TRANSACTIONS", IntegerType(), True),
        StructField("TOTAL_DEBIT_AMT", DecimalType(18, 2), True),
        StructField("TOTAL_CREDIT_AMT", DecimalType(18, 2), True),
        StructField("NET_CASH_FLOW", DecimalType(18, 2), True),
        StructField("AVG_TRANSACTION_SIZE", DecimalType(15, 2), True),
        StructField("MONTHLY_SPEND_TREND", StringType(), True),
        StructField("SPEND_PERCENTILE", DecimalType(5, 2), True),
        StructField("TOP_SPEND_CATEGORY", StringType(), True),
        StructField("DIGITAL_TXN_PCT", DecimalType(5, 2), True),
        StructField("FEE_INCOME", DecimalType(15, 2), True),
        StructField("INTEREST_INCOME", DecimalType(15, 2), True),
        StructField("REVENUE_CONTRIBUTION", DecimalType(15, 2), True),
        StructField("ANOMALY_FLAG", StringType(), True),
        StructField("MODEL_VERSION", StringType(), True),
        StructField("EFFECTIVE_DATE", DateType(), True),
        StructField("LOAD_TS", TimestampType(), True),
    ]
)


def proc_rank_groups(df: DataFrame, col: str, groups: int, out_col: str) -> DataFrame:
    """SAS PROC RANK groups=N: assigns floor(mean_tie_rank * groups / (n + 1)).

    rank() gives each tie group the min rank; SAS splits the tied ranks evenly,
    so mean rank = rank + (ties - 1) / 2. Nulls in `col` get null out_col.
    Uses a single global window (no partitioning), matching the SAS job.
    """
    n = df.where(F.col(col).isNotNull()).count()
    if n == 0:
        return df.withColumn(out_col, F.lit(None))
    w_rank = Window.orderBy(F.col(col).asc_nulls_last())
    w_ties = Window.partitionBy(col)
    return (
        df.withColumn("_rank", F.rank().over(w_rank))
        .withColumn("_ties", F.count("*").over(w_ties))
        .withColumn(
            out_col,
            F.when(
                F.col(col).isNotNull(),
                F.floor(
                    (F.col("_rank") + (F.col("_ties") - 1) / 2)
                    * F.lit(groups)
                    / F.lit(n + 1)
                ),
            ),
        )
        .drop("_rank", "_ties")
    )


def qntldef5_quantile(df: DataFrame, col: str, p):
    """SAS QNTLDEF=5 quantile of `col` over non-null values.

    Sorted values x(1..n), np = n*p, j = floor(np), g = np - j.
    If g == 0: (x(j) + x(j+1)) / 2, else x(j+1). Returns None when n == 0.
    `p` may be a float or Fraction; np is computed exactly (Fraction) so
    e.g. 0.25*n carries no float error. Only the needed sorted positions
    are collected, via a row_number window.
    """
    non_null = df.where(F.col(col).isNotNull())
    n = non_null.count()
    if n == 0:
        return None
    np_exact = Fraction(n) * (
        p if isinstance(p, Fraction) else Fraction(p).limit_denominator(10**9)
    )
    j = np_exact.numerator // np_exact.denominator
    g = np_exact - j
    positions = (j, j + 1) if g == 0 else (j + 1,)
    w = Window.orderBy(F.col(col).asc())
    rows = {
        r["rn"]: r["v"]
        for r in non_null.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn").isin(*positions))
        .select("rn", F.col(col).alias("v"))
        .collect()
    }
    if g == 0:
        xj = rows[j]
        xj1 = rows.get(j + 1, xj)  # j == n only when p >= 1
        return float(xj + xj1) / 2.0
    return float(rows[j + 1])


def transform(stg_txn_summary: DataFrame, as_of_date: date, **params) -> DataFrame:
    sum_debit = F.sum("AMT_TOTAL_DEBIT")
    sum_credit = F.sum("AMT_TOTAL_CREDIT")
    sum_txn = F.sum("TXN_COUNT_TOTAL")

    agg = stg_txn_summary.groupBy("CUSTOMER_ID").agg(
        F.countDistinct("ACCOUNT_ID").alias("TOTAL_ACCOUNTS"),
        F.sum(F.when(F.col("DAYS_SINCE_LAST_TXN") <= 30, 1).otherwise(0)).alias(
            "ACTIVE_ACCOUNTS"
        ),
        sum_txn.alias("TOTAL_TRANSACTIONS"),
        sum_debit.alias("TOTAL_DEBIT_AMT"),
        sum_credit.alias("TOTAL_CREDIT_AMT"),
        (sum_credit - sum_debit).alias("NET_CASH_FLOW"),
        F.when(sum_txn > 0, F.sum(F.col("AMT_TOTAL_DEBIT") + F.col("AMT_TOTAL_CREDIT")) / sum_txn)
        .otherwise(F.lit(0))
        .alias("_AVG_TXN_RAW"),
        F.sum("AMT_TOTAL_FEES").alias("TOTAL_FEES"),
        # SAS quirk: max() picks the lexicographically largest category, nulls ignored
        F.max("TOP_MERCHANT_CATEGORY").alias("TOP_SPEND_CATEGORY"),
        F.when(
            sum_txn > 0,
            F.sum(F.col("TXN_COUNT_TOTAL") * (F.col("PCT_WEB") + F.col("PCT_MOBILE")) / 100)
            / sum_txn
            * 100,
        )
        .otherwise(F.lit(0))
        .alias("DIGITAL_TXN_PCT"),
    )

    # Trend uses the unrounded average transaction size, as in the SAS DATA step
    agg = agg.withColumn(
        "MONTHLY_SPEND_TREND",
        F.when(F.col("NET_CASH_FLOW") > F.col("_AVG_TXN_RAW") * 5, "UP")
        .when(F.col("NET_CASH_FLOW") < -F.col("_AVG_TXN_RAW") * 5, "DOWN")
        .otherwise("STABLE"),
    ).withColumn(
        "_INTEREST_RAW", F.col("TOTAL_DEBIT_AMT") * Decimal("0.02")
    ).withColumn(
        "FEE_INCOME", F.col("TOTAL_FEES")
    ).withColumn(
        "_REVENUE_RAW", F.col("TOTAL_FEES") + F.col("_INTEREST_RAW")
    )

    ranked = proc_rank_groups(
        agg, "TOTAL_DEBIT_AMT", SPEND_PERCENTILE_GROUPS, "SPEND_PERCENTILE"
    )

    median = qntldef5_quantile(ranked, "TOTAL_DEBIT_AMT", Fraction(1, 2))
    q1 = qntldef5_quantile(ranked, "TOTAL_DEBIT_AMT", Fraction(1, 4))
    q3 = qntldef5_quantile(ranked, "TOTAL_DEBIT_AMT", Fraction(3, 4))
    iqr = (q3 - q1) if (q1 is not None and q3 is not None) else None
    threshold = (
        median + ANOMALY_IQR_MULTIPLIER * iqr
        if (median is not None and iqr is not None and iqr > 0)
        else None
    )
    anomaly = (
        F.when(F.col("TOTAL_DEBIT_AMT") > F.lit(threshold), F.lit("Y")).otherwise(F.lit("N"))
        if threshold is not None
        else F.lit("N")
    )

    period = as_of_date.strftime("%Y-%m")
    out = ranked.withColumn("ANOMALY_FLAG", anomaly).select(
        F.col("CUSTOMER_ID").cast(LongType()),
        F.lit(period).cast(StringType()).alias("REPORTING_PERIOD"),
        F.col("TOTAL_ACCOUNTS").cast(ShortType()),
        F.col("ACTIVE_ACCOUNTS").cast(ShortType()),
        F.col("TOTAL_TRANSACTIONS").cast(IntegerType()),
        F.col("TOTAL_DEBIT_AMT").cast(DecimalType(18, 2)),
        F.col("TOTAL_CREDIT_AMT").cast(DecimalType(18, 2)),
        F.col("NET_CASH_FLOW").cast(DecimalType(18, 2)),
        F.col("_AVG_TXN_RAW").cast(DecimalType(15, 2)).alias("AVG_TRANSACTION_SIZE"),
        F.col("MONTHLY_SPEND_TREND").cast(StringType()),
        F.col("SPEND_PERCENTILE").cast(DecimalType(5, 2)),
        F.col("TOP_SPEND_CATEGORY").cast(StringType()),
        F.col("DIGITAL_TXN_PCT").cast(DecimalType(5, 2)),
        F.col("FEE_INCOME").cast(DecimalType(15, 2)),
        F.col("_INTEREST_RAW").cast(DecimalType(15, 2)).alias("INTEREST_INCOME"),
        F.col("_REVENUE_RAW").cast(DecimalType(15, 2)).alias("REVENUE_CONTRIBUTION"),
        F.col("ANOMALY_FLAG").cast(StringType()),
        F.lit(MODEL_VERSION).cast(StringType()).alias("MODEL_VERSION"),
        F.lit(as_of_date).cast(DateType()).alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return out


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from synapse.spark.common import pipeline_utils as pu

    spark = SparkSession.builder.appName("sjd_txn_analytics").getOrCreate()
    params = pu.get_run_params(argv)
    period = params.as_of_date.strftime("%Y-%m")
    pu.log_step(spark, JOB_NAME, STEP_NAME, "START", msg=f"Period: {period}")

    src = pu.read_table(spark, "stg", "STG_TXN_SUMMARY")
    pu.log_step(
        spark, JOB_NAME, STEP_NAME, "SUCCESS",
        msg="Extracted STG_TXN_SUMMARY", row_count=src.count(),
    )

    out = transform(src, params.as_of_date)

    try:
        pu.validate_table(
            out,
            VALIDATION_KEY_COLS,
            VALIDATION_NOT_NULL_COLS,
            min_rows=VALIDATION_MIN_ROWS,
        )
    except pu.ValidationError as e:
        pu.log_step(spark, JOB_NAME, STEP_NAME, "FAILED", msg=str(e))
        raise

    pu.write_table(
        out, "dp", "TRANSACTION_ANALYTICS",
        mode="append", pre_delete_where=f"REPORTING_PERIOD = '{period}'",
    )
    pu.log_step(
        spark, JOB_NAME, STEP_NAME, "SUCCESS",
        msg="Pipeline complete", row_count=out.count(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
