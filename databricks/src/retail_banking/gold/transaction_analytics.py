from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import Window

OUT_COLS = ["customer_id", "reporting_period", "total_accounts",
            "active_accounts", "total_transactions", "total_debit_amt",
            "total_credit_amt", "net_cash_flow", "avg_transaction_size",
            "monthly_spend_trend", "spend_percentile", "top_spend_category",
            "digital_txn_pct", "fee_income", "interest_income",
            "revenue_contribution", "anomaly_flag", "model_version",
            "effective_date", "load_ts"]


def build_transaction_analytics(stg_txn_summary, run_date: date):
    """Port of sas/02_sas_txn_analytics.sas (phase3b)."""
    df = stg_txn_summary

    cust = df.groupBy("customer_id").agg(
        F.countDistinct("account_id").alias("total_accounts"),
        F.sum(F.when(F.col("days_since_last_txn") <= 30, 1).otherwise(0))
            .alias("active_accounts"),
        F.sum("txn_count_total").alias("total_transactions"),
        F.sum("amt_total_debit").alias("total_debit_amt"),
        F.sum("amt_total_credit").alias("total_credit_amt"),
        F.sum("amt_total_fees").alias("total_fees"),
    )

    # pandas .first() = first non-null, deterministic via account_id order
    w_first = Window.partitionBy("customer_id").orderBy("account_id")
    top_cat = (df.filter(F.col("top_merchant_category").isNotNull())
        .withColumn("_rn", F.row_number().over(w_first))
        .filter(F.col("_rn") == 1)
        .select("customer_id",
                F.col("top_merchant_category").alias("top_spend_category")))
    cust = cust.join(top_cat, "customer_id", "left")

    cust = cust.withColumn("net_cash_flow",
                           F.col("total_credit_amt") - F.col("total_debit_amt"))
    cust = cust.withColumn("avg_transaction_size", F.when(
        F.col("total_transactions") > 0,
        (F.col("total_debit_amt") + F.col("total_credit_amt"))
        / F.col("total_transactions")).otherwise(F.lit(0.0)))

    dig = (df.withColumn("digital_txns",
                         F.col("txn_count_total")
                         * (F.coalesce("pct_web", F.lit(0.0))
                            + F.coalesce("pct_mobile", F.lit(0.0))) / 100.0)
        .groupBy("customer_id")
        .agg(F.sum("digital_txns").alias("dig_txns"),
             F.sum("txn_count_total").alias("tot")))
    cust = cust.join(
        dig.select("customer_id",
                   F.when(F.col("tot") > 0,
                          F.col("dig_txns") / F.col("tot") * 100)
                    .otherwise(F.lit(0.0)).alias("digital_txn_pct")),
        "customer_id", "left")

    cust = cust.withColumn("monthly_spend_trend", F.when(
        F.col("net_cash_flow") > F.col("avg_transaction_size") * 5, "UP")
        .when(F.col("net_cash_flow") < -F.col("avg_transaction_size") * 5,
              "DOWN").otherwise("STABLE"))
    cust = cust.withColumn("fee_income", F.col("total_fees"))
    cust = cust.withColumn("interest_income",
                           F.round(F.col("total_debit_amt") * 0.02, 2))
    cust = cust.withColumn("revenue_contribution",
                           F.col("fee_income") + F.col("interest_income"))

    # pandas rank(pct=True, method='average') * 100
    w_rank = Window.orderBy("total_debit_amt")
    w_tie = Window.partitionBy("total_debit_amt")
    w_all = Window.rowsBetween(Window.unboundedPreceding,
                               Window.unboundedFollowing)
    cust = (cust
        .withColumn("_rn", F.row_number().over(w_rank))
        .withColumn("_avg_rank", F.avg("_rn").over(w_tie))
        .withColumn("_n", F.count("*").over(w_all))
        .withColumn("spend_percentile",
                    F.round(F.col("_avg_rank") / F.col("_n") * 100, 2))
        .drop("_rn", "_avg_rank", "_n"))

    q = cust.agg(F.expr(
        "percentile(total_debit_amt, array(0.25, 0.5, 0.75))").alias("q")
        ).first()["q"]
    q1, median, q3 = float(q[0]), float(q[1]), float(q[2])
    iqr = q3 - q1
    cust = cust.withColumn("anomaly_flag", F.when(
        (F.col("total_debit_amt") > F.lit(median + 3 * iqr))
        & (F.lit(iqr) > 0), "Y").otherwise("N"))

    cust = cust.withColumn("reporting_period",
                           F.date_format(F.lit(run_date), "yyyy-MM"))
    cust = cust.withColumn("model_version", F.lit("TXN_V2.1"))
    cust = cust.withColumn("effective_date", F.lit(run_date))
    cust = cust.withColumn("load_ts", F.current_timestamp())

    return cust.select(*OUT_COLS)
