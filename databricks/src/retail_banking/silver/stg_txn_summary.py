from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import Window


def build_stg_txn_summary(transactions, accounts, transaction_types,
                          run_date: date, lookback_months: int = 12):
    """Port of bteq/02_stg_txn_summary.bteq (phase2 2b)."""
    rd = F.lit(run_date)
    period_start = F.add_months(rd, -lookback_months)

    posted = (transactions
        .filter((F.col("transaction_date") >= period_start)
                & (F.col("transaction_date") <= rd)
                & (F.col("status_code") == "P")))

    top_cat = (posted
        .filter(F.col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(F.sum(F.abs("amount")).alias("cat_spend"))
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("account_id")
                  .orderBy(F.col("cat_spend").desc())))
        .filter(F.col("_rn") == 1)
        .select("account_id",
                F.col("merchant_category").alias("top_cat")))

    t = posted.join(accounts, "account_id")
    t = (t.join(transaction_types, "transaction_type_cd")
          .join(top_cat, "account_id", "left"))

    ch = lambda code: F.sum(
        F.when(F.col("channel_code") == code, 1).otherwise(0))
    pct = lambda code: (
        ch(code) * 100.0 / F.greatest(F.count("*"), F.lit(1))
    ).cast("decimal(5,2)")

    return (t.groupBy("customer_id", "account_id", "account_type", "top_cat")
        .agg(
            period_start.alias("summary_period_start"),
            rd.alias("summary_period_end"),
            F.count("*").alias("txn_count_total"),
            F.sum(F.when(F.col("category") == "DEBIT", 1).otherwise(0))
                .alias("txn_count_debit"),
            F.sum(F.when(F.col("category") == "CREDIT", 1).otherwise(0))
                .alias("txn_count_credit"),
            F.sum(F.when(F.col("category") == "FEE", 1).otherwise(0))
                .alias("txn_count_fee"),
            F.sum(F.when(F.col("category") == "DEBIT",
                         F.abs("amount")).otherwise(0))
                .alias("amt_total_debit"),
            F.sum(F.when(F.col("category") == "CREDIT",
                         F.col("amount")).otherwise(0))
                .alias("amt_total_credit"),
            F.sum(F.when(F.col("category") == "FEE",
                         F.abs("amount")).otherwise(0))
                .alias("amt_total_fees"),
            F.avg(F.when(F.col("category") == "DEBIT", F.abs("amount")))
                .alias("amt_avg_debit"),
            F.avg(F.when(F.col("category") == "CREDIT", F.col("amount")))
                .alias("amt_avg_credit"),
            F.max(F.when(F.col("category") == "DEBIT",
                         F.abs("amount")).otherwise(0))
                .alias("amt_max_single_debit"),
            F.max(F.when(F.col("category") == "CREDIT",
                         F.col("amount")).otherwise(0))
                .alias("amt_max_single_credit"),
            F.countDistinct("merchant_name").alias("distinct_merchants"),
            F.max("top_cat").alias("top_merchant_category"),
            pct("ATM").alias("pct_atm"),
            pct("POS").alias("pct_pos"),
            pct("WEB").alias("pct_web"),
            pct("MOB").alias("pct_mobile"),
            F.datediff(rd, F.max("transaction_date"))
                .alias("days_since_last_txn"),
            F.current_timestamp().alias("load_ts"),
        )
        .drop("top_cat"))
