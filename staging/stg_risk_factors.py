"""stg_risk_factors - PySpark port of ``bteq/03_stg_risk_factors.bteq``.

Computes risk-related feature vectors per customer for the downstream SAS risk
scoring model, combining balance behaviour, payment history, transaction
velocity, credit utilization, bureau scores and merchant-risk indicators.

Teradata -> Spark translation notes:
  * Two ``WRK_*`` intermediate ``MULTISET`` tables (daily balances, payment
    history) -> cached intermediate DataFrames (:func:`_daily_balance`,
    :func:`_payment_history`); the explicit ``DROP TABLE`` cleanup is implicit.
  * ``STDDEV_POP`` -> ``F.stddev_pop``; ``MONTHS_BETWEEN`` -> ``F.months_between``.
  * ``QUALIFY ROW_NUMBER() OVER (...)`` (latest daily balance / latest bureau
    score) -> ``Window`` + ``row_number()`` filter.
  * ``ADD_MONTHS`` / ``CURRENT_DATE - n`` -> ``F.add_months`` / ``F.date_sub``.
  * Correlated ``NOT IN`` subquery (new merchants for an account) -> a
    left-anti join against the account's prior merchant set.
  * ``CAST(... AS DECIMAL(5,4)/(5,2)/INTEGER)`` preserved exactly where the
    BTEQ casts; all other metrics keep full precision (as the BTEQ did).
  * ``COLLECT STATISTICS`` / ``PRIMARY INDEX`` / ``WITH DATA`` dropped.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from staging.config import StagingConfig
from staging.spark_utils import get_logger, read_all_sources, write_staging

TABLE_NAME = "stg_risk_factors"

HIGH_RISK_CATEGORIES = ["GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP"]

OUTPUT_COLUMNS = [
    "customer_id", "account_overdraft_cnt", "nsf_fee_total",
    "large_withdrawal_cnt", "large_withdrawal_amt", "avg_daily_balance_30d",
    "avg_daily_balance_90d", "balance_volatility", "credit_util_ratio",
    "payment_ontime_pct", "payment_late_cnt", "months_since_last_late",
    "external_credit_score", "debit_velocity_7d", "debit_velocity_30d",
    "new_merchant_cnt_30d", "international_txn_cnt", "high_risk_merchant_cnt",
    "load_ts",
]


def _daily_balance(transactions: DataFrame, accounts: DataFrame, as_of) -> DataFrame:
    """WRK_DAILY_BALANCE: last posted balance per account per day (~3 months)."""
    win = Window.partitionBy("account_id", "transaction_date").orderBy(
        F.col("transaction_ts").desc()
    )
    return (
        transactions.filter(
            (F.col("transaction_date") >= F.add_months(as_of, -3))
            & (F.col("status_code") == "P")
        )
        .join(accounts.select("account_id", "customer_id"), on="account_id", how="inner")
        .withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_id",
            "account_id",
            "transaction_date",
            F.col("running_balance").alias("eod_balance"),
        )
    )


def _payment_history(
    transactions: DataFrame, accounts: DataFrame, transaction_types: DataFrame, as_of
) -> DataFrame:
    """WRK_PAYMENT_HISTORY: on-time/late payment behaviour per customer/account."""
    base = (
        transactions.alias("t")
        .join(accounts.alias("acct"), on="account_id", how="inner")
        .join(transaction_types.alias("tt"), on="transaction_type_cd", how="inner")
        .filter(
            F.col("acct.account_type").isin("CREDIT", "LOAN")
            & (F.col("tt.category") == "CREDIT")
            & (F.col("t.status_code") == "P")
            & (F.col("t.transaction_date") >= F.add_months(as_of, -24))
        )
    )

    # Due-date proxy: OPEN_DATE + (floor(months since open) + 1) months.
    months_since_open = F.months_between(F.col("t.transaction_date"), F.col("acct.open_date")).cast("int")
    due_date = F.add_months(F.col("acct.open_date"), months_since_open + 1)
    is_late = F.col("t.transaction_date") > due_date

    enriched = base.select(
        F.col("acct.customer_id").alias("customer_id"),
        F.col("account_id"),
        F.col("acct.open_date").alias("open_date"),
        F.col("t.transaction_date").alias("transaction_date"),
        is_late.alias("is_late"),
    )

    return enriched.groupBy("customer_id", "account_id", "open_date").agg(
        F.count(F.lit(1)).alias("total_payments"),
        F.sum(F.when(~F.col("is_late"), 1).otherwise(0)).alias("ontime_payments"),
        F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_payments"),
        F.months_between(
            as_of,
            F.coalesce(
                F.max(F.when(F.col("is_late"), F.col("transaction_date"))),
                F.first("open_date"),
            ),
        ).cast("int").alias("months_since_last_late"),
    )


def transform(
    customers: DataFrame,
    accounts: DataFrame,
    transactions: DataFrame,
    transaction_types: DataFrame,
    bureau_scores: DataFrame,
    as_of_date: date,
) -> DataFrame:
    as_of = F.lit(as_of_date).cast("date")
    # DECIMAL(15,2) zero default keeps monetary aggregates exact (a DOUBLE
    # literal would promote the surrounding expression to DOUBLE).
    money_zero = F.lit(0).cast(DecimalType(15, 2))

    daily_balance = _daily_balance(transactions, accounts, as_of).cache()
    payment_history = _payment_history(transactions, accounts, transaction_types, as_of).cache()

    # -- Overdraft / NSF fees in the last 12 months --
    overdraft = (
        transactions.alias("t")
        .join(accounts.alias("acct"), on="account_id", how="inner")
        .join(transaction_types.alias("tt"), on="transaction_type_cd", how="inner")
        .filter(
            (F.col("t.transaction_date") >= F.add_months(as_of, -12))
            & (F.col("t.status_code") == "P")
        )
        .groupBy(F.col("acct.customer_id").alias("customer_id"))
        .agg(
            F.sum(F.when(F.col("t.running_balance") < 0, 1).otherwise(0)).alias("overdraft_count"),
            F.sum(
                F.when(
                    (F.col("tt.category") == "FEE") & F.col("tt.description").like("%NSF%"),
                    F.abs(F.col("t.amount")),
                ).otherwise(money_zero)
            ).alias("nsf_total"),
        )
    )

    # -- Large withdrawals (>= $5,000 single debit) in the last 12 months --
    large_wd = (
        transactions.alias("t")
        .join(accounts.alias("acct"), on="account_id", how="inner")
        .join(transaction_types.alias("tt"), on="transaction_type_cd", how="inner")
        .filter(
            (F.col("tt.category") == "DEBIT")
            & (F.abs(F.col("t.amount")) >= 5000)
            & (F.col("t.transaction_date") >= F.add_months(as_of, -12))
            & (F.col("t.status_code") == "P")
        )
        .groupBy(F.col("acct.customer_id").alias("customer_id"))
        .agg(
            F.count(F.lit(1)).alias("large_wd_cnt"),
            F.sum(F.abs(F.col("t.amount"))).alias("large_wd_amt"),
        )
    )

    # -- Average daily balance (30d / 90d) and volatility --
    bal = daily_balance.groupBy("customer_id").agg(
        F.avg(F.when(F.col("transaction_date") >= F.date_sub(as_of, 30), F.col("eod_balance"))).alias("avg_bal_30d"),
        F.avg(F.when(F.col("transaction_date") >= F.date_sub(as_of, 90), F.col("eod_balance"))).alias("avg_bal_90d"),
        F.stddev_pop(F.col("eod_balance")).alias("bal_stddev"),
    )

    # -- Credit utilization (open credit accounts) --
    credit = (
        accounts.filter((F.col("account_type") == "CREDIT") & (F.col("account_status") == "O"))
        .groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("current_balance"), money_zero)).alias("total_credit_bal"),
            F.sum(F.coalesce(F.col("credit_limit"), money_zero)).alias("total_credit_limit"),
        )
    )

    # -- Payment history summary per customer --
    pmh = payment_history.groupBy("customer_id").agg(
        F.sum("total_payments").alias("total_payments"),
        F.sum("ontime_payments").alias("ontime_payments"),
        F.sum("late_payments").alias("late_payments"),
        F.min("months_since_last_late").alias("months_since_last_late"),
    )

    # -- External bureau score (latest report per customer) --
    bureau_win = Window.partitionBy("customer_id").orderBy(F.col("report_date").desc())
    bureau = (
        bureau_scores.withColumn("_rn", F.row_number().over(bureau_win))
        .filter(F.col("_rn") == 1)
        .select("customer_id", F.col("external_credit_score").alias("credit_score"))
    )

    # -- Debit velocity (7d / 30d rolling totals) --
    vel = (
        transactions.alias("t")
        .join(accounts.alias("acct"), on="account_id", how="inner")
        .join(transaction_types.alias("tt"), on="transaction_type_cd", how="inner")
        .filter(
            (F.col("tt.category") == "DEBIT")
            & (F.col("t.transaction_date") >= F.date_sub(as_of, 30))
            & (F.col("t.status_code") == "P")
        )
        .groupBy(F.col("acct.customer_id").alias("customer_id"))
        .agg(
            F.sum(F.when(F.col("t.transaction_date") >= F.date_sub(as_of, 7), F.abs(F.col("t.amount"))).otherwise(money_zero)).alias("debit_7d"),
            F.sum(F.when(F.col("t.transaction_date") >= F.date_sub(as_of, 30), F.abs(F.col("t.amount"))).otherwise(money_zero)).alias("debit_30d"),
        )
    )

    # -- Merchant risk indicators (last 6 months) --
    base6 = (
        transactions.alias("t")
        .join(accounts.alias("acct"), on="account_id", how="inner")
        .filter(
            (F.col("t.transaction_date") >= F.add_months(as_of, -6))
            & (F.col("t.status_code") == "P")
        )
        .select(
            F.col("acct.customer_id").alias("customer_id"),
            F.col("account_id"),
            F.col("t.transaction_date").alias("transaction_date"),
            F.col("t.merchant_name").alias("merchant_name"),
            F.col("t.merchant_category").alias("merchant_category"),
            F.col("t.channel_code").alias("channel_code"),
        )
    )

    merch_counts = base6.groupBy("customer_id").agg(
        F.sum(F.when(F.col("channel_code") == "INTL", 1).otherwise(0)).alias("intl_txn_cnt"),
        F.sum(F.when(F.col("merchant_category").isin(HIGH_RISK_CATEGORIES), 1).otherwise(0)).alias("high_risk_cnt"),
    )

    # New merchants in last 30 days = merchant this account never transacted with
    # before (date < as_of - 30). Correlated NOT IN -> left-anti join.
    prior_merchants = (
        transactions.filter(
            (F.col("transaction_date") < F.date_sub(as_of, 30))
            & F.col("merchant_name").isNotNull()
        )
        .select("account_id", "merchant_name")
        .distinct()
    )
    new_merch = (
        base6.filter(
            (F.col("transaction_date") >= F.date_sub(as_of, 30))
            & F.col("merchant_name").isNotNull()
        )
        .join(prior_merchants, on=["account_id", "merchant_name"], how="left_anti")
        .groupBy("customer_id")
        .agg(F.countDistinct("merchant_name").alias("new_merch_30d"))
    )
    merch = merch_counts.join(new_merch, on="customer_id", how="left")

    credit_util_expr = (
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("total_credit_bal") / F.col("total_credit_limit")).cast(DecimalType(5, 4)),
        )
        .otherwise(F.lit(0.0000).cast(DecimalType(5, 4)))
    )
    payment_ontime_expr = (
        F.when(
            F.col("total_payments") > 0,
            (F.col("ontime_payments") * 100.0 / F.col("total_payments")).cast(DecimalType(5, 2)),
        )
        .otherwise(F.lit(100.00).cast(DecimalType(5, 2)))
    )

    result = (
        customers.alias("c")
        .filter(F.col("customer_status").isin("A", "I"))
        .join(overdraft, on="customer_id", how="left")
        .join(large_wd, on="customer_id", how="left")
        .join(bal, on="customer_id", how="left")
        .join(credit, on="customer_id", how="left")
        .join(pmh, on="customer_id", how="left")
        .join(bureau, on="customer_id", how="left")
        .join(vel, on="customer_id", how="left")
        .join(merch, on="customer_id", how="left")
        .select(
            F.col("customer_id"),
            F.coalesce(F.col("overdraft_count"), F.lit(0)).alias("account_overdraft_cnt"),
            F.coalesce(F.col("nsf_total"), money_zero).alias("nsf_fee_total"),
            F.coalesce(F.col("large_wd_cnt"), F.lit(0)).alias("large_withdrawal_cnt"),
            F.coalesce(F.col("large_wd_amt"), money_zero).alias("large_withdrawal_amt"),
            F.coalesce(F.col("avg_bal_30d"), money_zero).alias("avg_daily_balance_30d"),
            F.coalesce(F.col("avg_bal_90d"), money_zero).alias("avg_daily_balance_90d"),
            F.coalesce(F.col("bal_stddev"), F.lit(0.0000)).alias("balance_volatility"),
            credit_util_expr.alias("credit_util_ratio"),
            payment_ontime_expr.alias("payment_ontime_pct"),
            F.coalesce(F.col("late_payments"), F.lit(0)).alias("payment_late_cnt"),
            F.coalesce(F.col("months_since_last_late"), F.lit(999)).alias("months_since_last_late"),
            F.coalesce(F.col("credit_score"), F.lit(0)).alias("external_credit_score"),
            F.coalesce(F.col("debit_7d"), money_zero).alias("debit_velocity_7d"),
            F.coalesce(F.col("debit_30d"), money_zero).alias("debit_velocity_30d"),
            F.coalesce(F.col("new_merch_30d"), F.lit(0)).alias("new_merchant_cnt_30d"),
            F.coalesce(F.col("intl_txn_cnt"), F.lit(0)).alias("international_txn_cnt"),
            F.coalesce(F.col("high_risk_cnt"), F.lit(0)).alias("high_risk_merchant_cnt"),
            F.current_timestamp().alias("load_ts"),
        )
    )
    return result.select(*OUTPUT_COLUMNS)


def run(spark: SparkSession, cfg: StagingConfig) -> int:
    logger = get_logger(cfg, "staging.stg_risk_factors")
    logger.info("step start", step=TABLE_NAME, status="START")
    src = read_all_sources(spark, cfg)
    df = transform(
        src["customers"], src["accounts"], src["transactions"],
        src["transaction_types"], src["customer_bureau_scores"], cfg.as_of_date,
    )
    df = df.cache()
    row_count = df.count()
    write_staging(df, cfg, TABLE_NAME, logger)
    logger.info("step complete", step=TABLE_NAME, status="SUCCESS", row_count=row_count)
    return row_count
