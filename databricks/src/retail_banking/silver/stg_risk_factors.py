from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import Window

from ..date_utils import datediff_month


def build_stg_risk_factors(customers, accounts, transactions,
                           transaction_types, bureau_scores, run_date: date):
    """Port of bteq/03_stg_risk_factors.bteq (phase2 2c)."""
    rd = F.lit(run_date)

    # wrk_daily_balance: last posted txn per (account, day) over last 3 months
    wrk_daily_balance = (transactions
        .filter((F.col("transaction_date") >= F.add_months(rd, -3))
                & (F.col("status_code") == "P"))
        .join(accounts.select("account_id", "customer_id"), "account_id")
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("account_id", "transaction_date")
                  .orderBy(F.col("transaction_ts").desc())))
        .filter(F.col("_rn") == 1)
        .select("customer_id", "account_id", "transaction_date",
                F.col("running_balance").alias("eod_balance")))

    # wrk_payment_history: payments on CREDIT/LOAN accounts, last 24 months.
    # A payment is on time if it posts on or before the end of the calendar
    # month in which it occurs relative to open_date, i.e.
    # txn_date <= add_months(open_date, datediff_month(open_date, txn_date)+1)
    pay_base = (transactions
        .join(accounts.filter(F.col("account_type").isin("CREDIT", "LOAN"))
              .select("account_id", "customer_id", "account_type", "open_date"),
              "account_id")
        .join(transaction_types.filter(F.col("category") == "CREDIT")
              .select("transaction_type_cd"), "transaction_type_cd")
        .filter(F.col("status_code") == "P")
        .filter(F.col("transaction_date") >= F.add_months(rd, -24))
        .withColumn("_due", F.add_months(
            "open_date",
            datediff_month("open_date", "transaction_date") + 1))
        .withColumn("_late",
                    (F.col("transaction_date") > F.col("_due")).cast("int")))

    wrk_payment_history = (pay_base
        .groupBy("customer_id", "account_id")
        .agg(
            F.count("*").alias("total_payments"),
            F.sum(1 - F.col("_late")).alias("ontime_payments"),
            F.sum("_late").alias("late_payments"),
            datediff_month(
                F.coalesce(
                    F.max(F.when(F.col("_late") == 1,
                                 F.col("transaction_date"))),
                    F.min("open_date")),
                rd).cast("int").alias("months_since_last_late"),
        ))

    base = (transactions
        .join(accounts.select("account_id", "customer_id"), "account_id")
        .join(transaction_types.select("transaction_type_cd", "category",
                                       "description"),
              "transaction_type_cd")
        .filter(F.col("status_code") == "P")
        .filter(F.col("transaction_date") >= F.add_months(rd, -12)))

    overdraft = (base.groupBy("customer_id").agg(
        F.sum(F.when(F.col("running_balance") < 0, 1).otherwise(0))
            .alias("overdraft_count"),
        F.sum(F.when((F.col("category") == "FEE")
                     & F.col("description").like("%NSF%"),
                     F.abs("amount")).otherwise(0)).alias("nsf_total")))

    lg_wd = (base.filter((F.col("category") == "DEBIT")
                         & (F.abs("amount") >= 5000))
        .groupBy("customer_id").agg(
            F.count("*").alias("large_wd_cnt"),
            F.sum(F.abs("amount")).alias("large_wd_amt")))

    bal = (wrk_daily_balance.groupBy("customer_id").agg(
        F.avg(F.when(F.col("transaction_date") >= F.date_sub(rd, 30),
                     F.col("eod_balance"))).alias("avg_bal_30d"),
        F.avg(F.when(F.col("transaction_date") >= F.date_sub(rd, 90),
                     F.col("eod_balance"))).alias("avg_bal_90d"),
        F.stddev_pop("eod_balance").alias("bal_stddev")))

    credit = (accounts
        .filter((F.col("account_type") == "CREDIT")
                & (F.col("account_status") == "O"))
        .groupBy("customer_id").agg(
            F.sum(F.coalesce("current_balance", F.lit(0)))
                .alias("total_credit_bal"),
            F.sum(F.coalesce("credit_limit", F.lit(0)))
                .alias("total_credit_limit")))

    pmh = (wrk_payment_history.groupBy("customer_id").agg(
        F.sum("total_payments").alias("total_payments"),
        F.sum("ontime_payments").alias("ontime_payments"),
        F.sum("late_payments").alias("late_payments"),
        F.min("months_since_last_late").alias("months_since_last_late")))

    bureau = (bureau_scores
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("customer_id")
                  .orderBy(F.col("report_date").desc())))
        .filter(F.col("_rn") == 1)
        .select("customer_id",
                F.col("external_credit_score").alias("credit_score")))

    vel = (transactions
        .join(accounts.select("account_id", "customer_id"), "account_id")
        .join(transaction_types.filter(F.col("category") == "DEBIT")
              .select("transaction_type_cd"), "transaction_type_cd")
        .filter(F.col("status_code") == "P")
        .filter(F.col("transaction_date") >= F.date_sub(rd, 30))
        .groupBy("customer_id").agg(
            F.sum(F.when(F.col("transaction_date") >= F.date_sub(rd, 7),
                         F.abs("amount")).otherwise(0)).alias("debit_7d"),
            F.sum(F.abs("amount")).alias("debit_30d")))

    merch = (transactions
        .join(accounts.select("account_id", "customer_id"), "account_id")
        .filter(F.col("status_code") == "P")
        .filter(F.col("transaction_date") >= F.add_months(rd, -6))
        .groupBy("customer_id").agg(
            F.countDistinct(F.when(
                F.col("transaction_date") >= F.date_sub(rd, 30),
                F.col("merchant_name"))).alias("new_merch_30d"),
            F.sum(F.when(F.col("channel_code") == "INTL", 1).otherwise(0))
                .alias("intl_txn_cnt"),
            F.sum(F.when(F.col("merchant_category").isin(
                "GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE",
                "PAWN_SHOP"), 1).otherwise(0)).alias("high_risk_cnt")))

    return (customers
        .filter(F.col("customer_status").isin("A", "I"))
        .join(overdraft, "customer_id", "left")
        .join(lg_wd, "customer_id", "left")
        .join(bal, "customer_id", "left")
        .join(credit, "customer_id", "left")
        .join(pmh, "customer_id", "left")
        .join(bureau, "customer_id", "left")
        .join(vel, "customer_id", "left")
        .join(merch, "customer_id", "left")
        .select(
            "customer_id",
            F.coalesce("overdraft_count", F.lit(0))
                .alias("account_overdraft_cnt"),
            F.coalesce("nsf_total", F.lit(0.00)).alias("nsf_fee_total"),
            F.coalesce("large_wd_cnt", F.lit(0))
                .alias("large_withdrawal_cnt"),
            F.coalesce("large_wd_amt", F.lit(0.00))
                .alias("large_withdrawal_amt"),
            F.coalesce("avg_bal_30d", F.lit(0.00))
                .alias("avg_daily_balance_30d"),
            F.coalesce("avg_bal_90d", F.lit(0.00))
                .alias("avg_daily_balance_90d"),
            F.coalesce("bal_stddev", F.lit(0.0000))
                .alias("balance_volatility"),
            F.when(F.col("total_credit_limit") > 0,
                   (F.col("total_credit_bal") / F.col("total_credit_limit"))
                   .cast("decimal(5,4)"))
             .otherwise(F.lit(0.0000).cast("decimal(5,4)"))
             .alias("credit_util_ratio"),
            F.when(F.col("total_payments") > 0,
                   (F.col("ontime_payments") * 100.0 / F.col("total_payments"))
                   .cast("decimal(5,2)"))
             .otherwise(F.lit(100.00).cast("decimal(5,2)"))
             .alias("payment_ontime_pct"),
            F.coalesce("late_payments", F.lit(0)).alias("payment_late_cnt"),
            F.coalesce("months_since_last_late", F.lit(999))
                .alias("months_since_last_late"),
            F.coalesce("credit_score", F.lit(0))
                .alias("external_credit_score"),
            F.coalesce("debit_7d", F.lit(0.00)).alias("debit_velocity_7d"),
            F.coalesce("debit_30d", F.lit(0.00)).alias("debit_velocity_30d"),
            F.coalesce("new_merch_30d", F.lit(0))
                .alias("new_merchant_cnt_30d"),
            F.coalesce("intl_txn_cnt", F.lit(0))
                .alias("international_txn_cnt"),
            F.coalesce("high_risk_cnt", F.lit(0))
                .alias("high_risk_merchant_cnt"),
            F.current_timestamp().alias("load_ts"),
        ))
