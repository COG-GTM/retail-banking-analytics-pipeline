"""Ticket 6 - Risk-factor staging.

PySpark port of ``bteq/03_stg_risk_factors.bteq``.

Inputs  : core_banking.customers, core_banking.accounts,
          core_banking.customer_bureau_scores,
          txn_processing.transactions, txn_processing.transaction_types
Output  : etl_staging.stg_risk_factors  (Delta)

The two Teradata work tables ``WRK_DAILY_BALANCE`` and ``WRK_PAYMENT_HISTORY``
are replaced by cached DataFrames (``.cache()``) instead of persisted tables.
"""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HIGH_RISK_CATEGORIES = ["GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP"]

OUTPUT_COLUMNS = [
    "customer_id", "account_overdraft_cnt", "nsf_fee_total", "large_withdrawal_cnt",
    "large_withdrawal_amt", "avg_daily_balance_30d", "avg_daily_balance_90d",
    "balance_volatility", "credit_util_ratio", "payment_ontime_pct", "payment_late_cnt",
    "months_since_last_late", "external_credit_score", "debit_velocity_7d",
    "debit_velocity_30d", "new_merchant_cnt_30d", "international_txn_cnt",
    "high_risk_merchant_cnt", "load_ts",
]


def build_wrk_daily_balance(
    transactions: DataFrame, accounts: DataFrame, run_date: date
) -> DataFrame:
    """WRK_DAILY_BALANCE: last posted txn per account/day over the last 3 months."""
    w = Window.partitionBy("account_id", "transaction_date").orderBy(
        F.col("transaction_ts").desc()
    )
    return (
        transactions.where(
            (F.col("status_code") == "P")
            & (F.col("transaction_date") >= F.add_months(F.lit(run_date), -3))
        )
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .join(accounts.select("account_id", "customer_id"), "account_id", "inner")
        .select(
            "customer_id",
            "account_id",
            "transaction_date",
            F.col("running_balance").alias("eod_balance"),
        )
    )


def build_wrk_payment_history(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    run_date: date,
) -> DataFrame:
    """WRK_PAYMENT_HISTORY: payment behaviour on CREDIT/LOAN accounts (24 months)."""
    months_elapsed = F.months_between(F.col("transaction_date"), F.col("open_date")).cast("int")
    due_date = F.add_months(F.col("open_date"), months_elapsed + 1)
    is_ontime = F.col("transaction_date") <= due_date
    is_late = F.col("transaction_date") > due_date

    src = (
        transactions.where(
            (F.col("status_code") == "P")
            & (F.col("transaction_date") >= F.add_months(F.lit(run_date), -24))
        )
        .join(
            accounts.select("account_id", "customer_id", "account_type", "open_date"),
            "account_id",
            "inner",
        )
        .join(transaction_types.select("transaction_type_cd", "category"), "transaction_type_cd", "inner")
        .where(F.col("account_type").isin("CREDIT", "LOAN") & (F.col("category") == "CREDIT"))
    )

    last_late_date = F.max(F.when(is_late, F.col("transaction_date")))
    return src.groupBy("customer_id", "account_id", "open_date").agg(
        F.count(F.lit(1)).alias("total_payments"),
        F.sum(F.when(is_ontime, 1).otherwise(0)).alias("ontime_payments"),
        F.sum(F.when(is_late, 1).otherwise(0)).alias("late_payments"),
        F.months_between(
            F.lit(run_date), F.coalesce(last_late_date, F.col("open_date"))
        ).cast("int").alias("months_since_last_late"),
    )


def build_stg_risk_factors(
    customers: DataFrame,
    accounts: DataFrame,
    transactions: DataFrame,
    transaction_types: DataFrame,
    bureau_scores: DataFrame,
    run_date: date,
    load_ts: datetime,
) -> DataFrame:
    tt = transaction_types.select("transaction_type_cd", "category", "description")
    acct = accounts.select("account_id", "customer_id", "account_type", "account_status")
    run_lit = F.lit(run_date)

    posted = transactions.where(F.col("status_code") == "P")

    wrk_daily_balance = build_wrk_daily_balance(transactions, accounts, run_date).cache()
    wrk_payment_history = build_wrk_payment_history(
        transactions, accounts, transaction_types, run_date
    ).cache()

    # -- Overdraft / NSF fees in last 12 months --
    overdraft = (
        posted.where(F.col("transaction_date") >= F.add_months(run_lit, -12))
        .join(acct, "account_id", "inner")
        .join(tt, "transaction_type_cd", "inner")
        .groupBy("customer_id")
        .agg(
            F.sum(F.when(F.col("running_balance") < 0, 1).otherwise(0)).alias("overdraft_count"),
            F.sum(
                F.when(
                    (F.col("category") == "FEE") & F.col("description").like("%NSF%"),
                    F.abs(F.col("amount")),
                ).otherwise(0)
            ).alias("nsf_total"),
        )
    )

    # -- Large withdrawals (>= $5,000 single debit) --
    lg_wd = (
        posted.where(F.col("transaction_date") >= F.add_months(run_lit, -12))
        .join(acct, "account_id", "inner")
        .join(tt, "transaction_type_cd", "inner")
        .where((F.col("category") == "DEBIT") & (F.abs(F.col("amount")) >= 5000))
        .groupBy("customer_id")
        .agg(
            F.count(F.lit(1)).alias("large_wd_cnt"),
            F.sum(F.abs(F.col("amount"))).alias("large_wd_amt"),
        )
    )

    # -- Average daily balance (30/90 day) and volatility --
    bal = wrk_daily_balance.groupBy("customer_id").agg(
        F.avg(
            F.when(F.col("transaction_date") >= F.date_sub(run_lit, 30), F.col("eod_balance"))
        ).alias("avg_bal_30d"),
        F.avg(
            F.when(F.col("transaction_date") >= F.date_sub(run_lit, 90), F.col("eod_balance"))
        ).alias("avg_bal_90d"),
        F.stddev_pop(F.col("eod_balance")).alias("bal_stddev"),
    )

    # -- Credit utilization ratio --
    credit = (
        accounts.where((F.col("account_type") == "CREDIT") & (F.col("account_status") == "O"))
        .groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("current_balance"), F.lit(0))).alias("total_credit_bal"),
            F.sum(F.coalesce(F.col("credit_limit"), F.lit(0))).alias("total_credit_limit"),
        )
    )

    # -- Payment history summary --
    pmh = wrk_payment_history.groupBy("customer_id").agg(
        F.sum("total_payments").alias("total_payments"),
        F.sum("ontime_payments").alias("ontime_payments"),
        F.sum("late_payments").alias("late_payments"),
        F.min("months_since_last_late").alias("months_since_last_late"),
    )

    # -- External bureau score (latest report per customer) --
    bureau_w = Window.partitionBy("customer_id").orderBy(F.col("report_date").desc())
    bureau = (
        bureau_scores.withColumn("_rn", F.row_number().over(bureau_w))
        .where(F.col("_rn") == 1)
        .select("customer_id", F.col("external_credit_score").alias("credit_score"))
    )

    # -- Debit velocity (7-day and 30-day) --
    vel = (
        posted.where(F.col("transaction_date") >= F.date_sub(run_lit, 30))
        .join(acct, "account_id", "inner")
        .join(tt, "transaction_type_cd", "inner")
        .where(F.col("category") == "DEBIT")
        .groupBy("customer_id")
        .agg(
            F.sum(
                F.when(F.col("transaction_date") >= F.date_sub(run_lit, 7), F.abs(F.col("amount")))
                .otherwise(0)
            ).alias("debit_7d"),
            F.sum(F.abs(F.col("amount"))).alias("debit_30d"),
        )
    )

    # -- Merchant risk indicators (6-month window) --
    base6 = (
        posted.where(F.col("transaction_date") >= F.add_months(run_lit, -6))
        .join(acct.select("account_id", "customer_id"), "account_id", "inner")
    )
    merch_counts = base6.groupBy("customer_id").agg(
        F.sum(F.when(F.col("channel_code") == "INTL", 1).otherwise(0)).alias("intl_txn_cnt"),
        F.sum(
            F.when(F.col("merchant_category").isin(HIGH_RISK_CATEGORIES), 1).otherwise(0)
        ).alias("high_risk_cnt"),
    )

    prior_pairs = (
        transactions.where(
            (F.col("transaction_date") < F.date_sub(run_lit, 30))
            & F.col("merchant_name").isNotNull()
        )
        .select("account_id", "merchant_name")
        .distinct()
    )
    recent_pairs = (
        base6.where(
            (F.col("transaction_date") >= F.date_sub(run_lit, 30))
            & F.col("merchant_name").isNotNull()
        )
        .select("customer_id", "account_id", "merchant_name")
        .distinct()
    )
    new_merch = (
        recent_pairs.join(prior_pairs, ["account_id", "merchant_name"], "left_anti")
        .groupBy("customer_id")
        .agg(F.countDistinct("merchant_name").alias("new_merch_30d"))
    )

    credit_util_ratio = (
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("total_credit_bal") / F.col("total_credit_limit")).cast("decimal(5,4)"),
        )
        .otherwise(F.lit(0.0000).cast("decimal(5,4)"))
    )
    payment_ontime_pct = (
        F.when(
            F.col("total_payments") > 0,
            (F.col("ontime_payments") * 100.0 / F.col("total_payments")).cast("decimal(5,2)"),
        )
        .otherwise(F.lit(100.00).cast("decimal(5,2)"))
    )

    result = (
        customers.where(F.col("customer_status").isin("A", "I"))
        .select("customer_id")
        .join(overdraft, "customer_id", "left")
        .join(lg_wd, "customer_id", "left")
        .join(bal, "customer_id", "left")
        .join(credit, "customer_id", "left")
        .join(pmh, "customer_id", "left")
        .join(bureau, "customer_id", "left")
        .join(vel, "customer_id", "left")
        .join(merch_counts, "customer_id", "left")
        .join(new_merch, "customer_id", "left")
        .select(
            F.col("customer_id"),
            F.coalesce(F.col("overdraft_count"), F.lit(0)).cast("int").alias("account_overdraft_cnt"),
            F.coalesce(F.col("nsf_total"), F.lit(0.00)).cast("decimal(15,2)").alias("nsf_fee_total"),
            F.coalesce(F.col("large_wd_cnt"), F.lit(0)).cast("int").alias("large_withdrawal_cnt"),
            F.coalesce(F.col("large_wd_amt"), F.lit(0.00)).cast("decimal(18,2)").alias("large_withdrawal_amt"),
            F.coalesce(F.col("avg_bal_30d"), F.lit(0.00)).cast("decimal(15,2)").alias("avg_daily_balance_30d"),
            F.coalesce(F.col("avg_bal_90d"), F.lit(0.00)).cast("decimal(15,2)").alias("avg_daily_balance_90d"),
            F.coalesce(F.col("bal_stddev"), F.lit(0.0000)).cast("decimal(10,4)").alias("balance_volatility"),
            credit_util_ratio.alias("credit_util_ratio"),
            payment_ontime_pct.alias("payment_ontime_pct"),
            F.coalesce(F.col("late_payments"), F.lit(0)).cast("int").alias("payment_late_cnt"),
            F.coalesce(F.col("months_since_last_late"), F.lit(999)).cast("int").alias("months_since_last_late"),
            F.coalesce(F.col("credit_score"), F.lit(0)).cast("int").alias("external_credit_score"),
            F.coalesce(F.col("debit_7d"), F.lit(0.00)).cast("decimal(15,2)").alias("debit_velocity_7d"),
            F.coalesce(F.col("debit_30d"), F.lit(0.00)).cast("decimal(15,2)").alias("debit_velocity_30d"),
            F.coalesce(F.col("new_merch_30d"), F.lit(0)).cast("int").alias("new_merchant_cnt_30d"),
            F.coalesce(F.col("intl_txn_cnt"), F.lit(0)).cast("int").alias("international_txn_cnt"),
            F.coalesce(F.col("high_risk_cnt"), F.lit(0)).cast("int").alias("high_risk_merchant_cnt"),
            F.lit(load_ts).cast("timestamp").alias("load_ts"),
        )
    )
    return result
