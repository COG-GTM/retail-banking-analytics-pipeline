"""Compute per-customer risk feature vectors (silver / staging).

PySpark port of ``bteq/03_stg_risk_factors.bteq``. Writes the Delta table
``etl_staging.stg_risk_factors``, consumed downstream by the risk-scoring job.

Legacy construct -> PySpark mapping
-----------------------------------
* ``WRK_DAILY_BALANCE``  (Teradata work table) -> cached DataFrame + temp view
  ``wrk_daily_balance`` (last-per-day EOD balance snapshots, 3-month window).
* ``WRK_PAYMENT_HISTORY`` (Teradata work table) -> cached DataFrame + temp view
  ``wrk_payment_history`` (per-account payment behaviour on CREDIT/LOAN accounts).
* ``QUALIFY ROW_NUMBER() ... = 1`` -> ``Window`` + ``row_number`` filter.
* ``STDDEV_POP``       -> ``F.stddev_pop``.
* Correlated ``NOT IN`` new-merchant subquery -> left-anti join on
  ``(account_id, merchant_name)`` against prior history.
* ``ADD_MONTHS`` / ``CURRENT_DATE - n`` -> ``add_months`` / ``date_sub`` anchored
  on ``cfg.run_date`` (deterministic, re-runnable).

The write is an idempotent Delta ``overwrite``; the step is wrapped with
``log_step`` and its output validated (not-null + unique ``customer_id``).
"""
from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.audit import init_audit, log_step
from common.config import Config
from common.validation import validate_table

JOB_NAME = "03_stg_risk_factors"

HIGH_RISK_MERCHANT_CATEGORIES = (
    "GAMBLING",
    "WIRE_TRANSFER_INTL",
    "CRYPTO_EXCHANGE",
    "PAWN_SHOP",
)


def _source(spark: SparkSession, cfg: Config, schema: str, name: str) -> DataFrame:
    return spark.table(cfg.table(schema, name))


def _build_daily_balance(
    spark: SparkSession, cfg: Config, transactions: DataFrame, accounts: DataFrame, ref_date
) -> DataFrame:
    """Last posted EOD balance per account per day over the last 3 months.

    Ports the ``WRK_DAILY_BALANCE`` work table.
    """
    joined = (
        transactions.alias("t")
        .join(accounts.alias("acct"), F.col("t.account_id") == F.col("acct.account_id"))
        .where(
            (F.col("t.transaction_date") >= F.add_months(ref_date, -3))
            & (F.col("t.status_code") == F.lit("P"))
        )
        .select(
            F.col("acct.customer_id").alias("customer_id"),
            F.col("t.account_id").alias("account_id"),
            F.col("t.transaction_date").alias("transaction_date"),
            F.col("t.transaction_ts").alias("transaction_ts"),
            F.col("t.running_balance").alias("eod_balance"),
        )
    )
    last_of_day = Window.partitionBy("account_id", "transaction_date").orderBy(
        F.col("transaction_ts").desc()
    )
    daily = (
        joined.withColumn("_rn", F.row_number().over(last_of_day))
        .where(F.col("_rn") == 1)
        .drop("_rn", "transaction_ts")
    )
    daily = daily.cache()
    daily.createOrReplaceTempView("wrk_daily_balance")
    return daily


def _build_payment_history(
    transactions: DataFrame, accounts: DataFrame, txn_types: DataFrame, ref_date
) -> DataFrame:
    """Per-account payment behaviour on CREDIT/LOAN accounts.

    Ports the ``WRK_PAYMENT_HISTORY`` work table. A payment is "on-time" when it
    posts within one month of its scheduled cadence proxy
    (``open_date + floor(months_elapsed) + 1`` months).
    """
    payments = (
        transactions.alias("t")
        .join(accounts.alias("acct"), F.col("t.account_id") == F.col("acct.account_id"))
        .join(
            txn_types.alias("tt"),
            F.col("t.transaction_type_cd") == F.col("tt.transaction_type_cd"),
        )
        .where(
            F.col("acct.account_type").isin("CREDIT", "LOAN")
            & (F.col("tt.category") == F.lit("CREDIT"))
            & (F.col("t.status_code") == F.lit("P"))
            & (F.col("t.transaction_date") >= F.add_months(ref_date, -24))
        )
        .select(
            F.col("acct.customer_id").alias("customer_id"),
            F.col("acct.account_id").alias("account_id"),
            F.col("acct.open_date").alias("open_date"),
            F.col("t.transaction_date").alias("transaction_date"),
        )
    )

    months_elapsed = F.floor(
        F.months_between(F.col("transaction_date"), F.col("open_date"))
    ).cast("int")
    due_date = F.expr("add_months(open_date, cast(months_elapsed as int) + 1)")
    payments = payments.withColumn("months_elapsed", months_elapsed).withColumn(
        "due_date", due_date
    )
    is_late = F.col("transaction_date") > F.col("due_date")
    payments = payments.withColumn("is_late", is_late)

    history = payments.groupBy("customer_id", "account_id").agg(
        F.count(F.lit(1)).alias("total_payments"),
        F.sum(F.when(~F.col("is_late"), 1).otherwise(0)).alias("ontime_payments"),
        F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_payments"),
        F.max(F.when(F.col("is_late"), F.col("transaction_date"))).alias(
            "last_late_date"
        ),
        F.first("open_date").alias("open_date"),
    )
    history = history.withColumn(
        "months_since_last_late",
        F.months_between(
            ref_date, F.coalesce(F.col("last_late_date"), F.col("open_date"))
        ).cast("int"),
    ).drop("last_late_date", "open_date")

    history = history.cache()
    history.createOrReplaceTempView("wrk_payment_history")
    return history


def _overdraft_nsf(transactions, accounts, txn_types, ref_date) -> DataFrame:
    return (
        transactions.alias("t")
        .join(accounts.alias("acct"), F.col("t.account_id") == F.col("acct.account_id"))
        .join(
            txn_types.alias("tt"),
            F.col("t.transaction_type_cd") == F.col("tt.transaction_type_cd"),
        )
        .where(
            (F.col("t.transaction_date") >= F.add_months(ref_date, -12))
            & (F.col("t.status_code") == F.lit("P"))
        )
        .groupBy(F.col("acct.customer_id").alias("customer_id"))
        .agg(
            F.sum(F.when(F.col("t.running_balance") < 0, 1).otherwise(0)).alias(
                "overdraft_count"
            ),
            F.sum(
                F.when(
                    (F.col("tt.category") == F.lit("FEE"))
                    & (F.col("tt.description").like("%NSF%")),
                    F.abs(F.col("t.amount")),
                ).otherwise(0)
            ).alias("nsf_total"),
        )
    )


def _large_withdrawals(transactions, accounts, txn_types, ref_date) -> DataFrame:
    return (
        transactions.alias("t")
        .join(accounts.alias("acct"), F.col("t.account_id") == F.col("acct.account_id"))
        .join(
            txn_types.alias("tt"),
            F.col("t.transaction_type_cd") == F.col("tt.transaction_type_cd"),
        )
        .where(
            (F.col("tt.category") == F.lit("DEBIT"))
            & (F.abs(F.col("t.amount")) >= F.lit(5000))
            & (F.col("t.transaction_date") >= F.add_months(ref_date, -12))
            & (F.col("t.status_code") == F.lit("P"))
        )
        .groupBy(F.col("acct.customer_id").alias("customer_id"))
        .agg(
            F.count(F.lit(1)).alias("large_wd_cnt"),
            F.sum(F.abs(F.col("t.amount"))).alias("large_wd_amt"),
        )
    )


def _balance_metrics(daily_balance: DataFrame, ref_date) -> DataFrame:
    return daily_balance.groupBy("customer_id").agg(
        F.avg(
            F.when(
                F.col("transaction_date") >= F.date_sub(ref_date, 30),
                F.col("eod_balance"),
            )
        ).alias("avg_bal_30d"),
        F.avg(
            F.when(
                F.col("transaction_date") >= F.date_sub(ref_date, 90),
                F.col("eod_balance"),
            )
        ).alias("avg_bal_90d"),
        F.stddev_pop(F.col("eod_balance")).alias("bal_stddev"),
    )


def _credit_utilization(accounts: DataFrame) -> DataFrame:
    agg = (
        accounts.where(
            (F.col("account_type") == F.lit("CREDIT"))
            & (F.col("account_status") == F.lit("O"))
        )
        .groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("current_balance"), F.lit(0))).alias(
                "total_credit_bal"
            ),
            F.sum(F.coalesce(F.col("credit_limit"), F.lit(0))).alias(
                "total_credit_limit"
            ),
        )
    )
    return agg.withColumn(
        "credit_util_ratio",
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("total_credit_bal") / F.col("total_credit_limit")).cast(
                "decimal(5,4)"
            ),
        ).otherwise(F.lit(0.0).cast("decimal(5,4)")),
    ).select("customer_id", "credit_util_ratio")


def _payment_summary(payment_history: DataFrame) -> DataFrame:
    return payment_history.groupBy("customer_id").agg(
        F.sum("total_payments").alias("total_payments"),
        F.sum("ontime_payments").alias("ontime_payments"),
        F.sum("late_payments").alias("late_payments"),
        F.min("months_since_last_late").alias("months_since_last_late"),
    )


def _bureau_scores(bureau: DataFrame) -> DataFrame:
    latest = Window.partitionBy("customer_id").orderBy(F.col("report_date").desc())
    return (
        bureau.withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .select(
            "customer_id",
            F.col("external_credit_score").alias("credit_score"),
        )
    )


def _debit_velocity(transactions, accounts, txn_types, ref_date) -> DataFrame:
    return (
        transactions.alias("t")
        .join(accounts.alias("acct"), F.col("t.account_id") == F.col("acct.account_id"))
        .join(
            txn_types.alias("tt"),
            F.col("t.transaction_type_cd") == F.col("tt.transaction_type_cd"),
        )
        .where(
            (F.col("tt.category") == F.lit("DEBIT"))
            & (F.col("t.transaction_date") >= F.date_sub(ref_date, 30))
            & (F.col("t.status_code") == F.lit("P"))
        )
        .groupBy(F.col("acct.customer_id").alias("customer_id"))
        .agg(
            F.sum(
                F.when(
                    F.col("t.transaction_date") >= F.date_sub(ref_date, 7),
                    F.abs(F.col("t.amount")),
                ).otherwise(0)
            ).alias("debit_7d"),
            F.sum(
                F.when(
                    F.col("t.transaction_date") >= F.date_sub(ref_date, 30),
                    F.abs(F.col("t.amount")),
                ).otherwise(0)
            ).alias("debit_30d"),
        )
    )


def _merchant_risk(transactions, accounts, ref_date) -> DataFrame:
    """New-merchant / international / high-risk-merchant counts (6-month window)."""
    window_txns = (
        transactions.alias("t")
        .join(accounts.alias("acct"), F.col("t.account_id") == F.col("acct.account_id"))
        .where(
            (F.col("t.transaction_date") >= F.add_months(ref_date, -6))
            & (F.col("t.status_code") == F.lit("P"))
        )
        .select(
            F.col("acct.customer_id").alias("customer_id"),
            F.col("t.account_id").alias("account_id"),
            F.col("t.transaction_date").alias("transaction_date"),
            F.col("t.merchant_name").alias("merchant_name"),
            F.col("t.merchant_category").alias("merchant_category"),
            F.col("t.channel_code").alias("channel_code"),
        )
    )

    # Merchants each account transacted with BEFORE the 30-day window (any status,
    # full history) -> used to identify genuinely new merchants.
    prior_merchants = (
        transactions.where(
            (F.col("transaction_date") < F.date_sub(ref_date, 30))
            & F.col("merchant_name").isNotNull()
        )
        .select("account_id", "merchant_name")
        .distinct()
    )

    recent = window_txns.where(
        (F.col("transaction_date") >= F.date_sub(ref_date, 30))
        & F.col("merchant_name").isNotNull()
    ).select("customer_id", "account_id", "merchant_name")

    new_merchants = recent.join(
        prior_merchants, on=["account_id", "merchant_name"], how="left_anti"
    )
    new_merch_counts = new_merchants.groupBy("customer_id").agg(
        F.countDistinct("merchant_name").alias("new_merch_30d")
    )

    other_counts = window_txns.groupBy("customer_id").agg(
        F.sum(
            F.when(F.col("channel_code") == F.lit("INTL"), 1).otherwise(0)
        ).alias("intl_txn_cnt"),
        F.sum(
            F.when(
                F.col("merchant_category").isin(*HIGH_RISK_MERCHANT_CATEGORIES),
                1,
            ).otherwise(0)
        ).alias("high_risk_cnt"),
    )

    return other_counts.join(new_merch_counts, on="customer_id", how="left")


def build_risk_factors(spark: SparkSession, cfg: Config) -> DataFrame:
    """Assemble the ``stg_risk_factors`` DataFrame (no write side-effects)."""
    ref_date = F.to_date(F.lit(cfg.run_date))

    customers = _source(spark, cfg, cfg.schema_core, "customers")
    accounts = _source(spark, cfg, cfg.schema_core, "accounts")
    bureau = _source(spark, cfg, cfg.schema_core, "customer_bureau_scores")
    transactions = _source(spark, cfg, cfg.schema_txn, "transactions")
    txn_types = _source(spark, cfg, cfg.schema_txn, "transaction_types")

    daily_balance = _build_daily_balance(
        spark, cfg, transactions, accounts, ref_date
    )
    payment_history = _build_payment_history(
        transactions, accounts, txn_types, ref_date
    )

    overdraft = _overdraft_nsf(transactions, accounts, txn_types, ref_date)
    lg_wd = _large_withdrawals(transactions, accounts, txn_types, ref_date)
    bal = _balance_metrics(daily_balance, ref_date)
    credit = _credit_utilization(accounts)
    pmh = _payment_summary(payment_history)
    bureau_latest = _bureau_scores(bureau)
    vel = _debit_velocity(transactions, accounts, txn_types, ref_date)
    merch = _merchant_risk(transactions, accounts, ref_date)

    base = customers.where(F.col("customer_status").isin("A", "I")).select(
        "customer_id"
    )

    result = (
        base.join(overdraft, on="customer_id", how="left")
        .join(lg_wd, on="customer_id", how="left")
        .join(bal, on="customer_id", how="left")
        .join(credit, on="customer_id", how="left")
        .join(pmh, on="customer_id", how="left")
        .join(bureau_latest, on="customer_id", how="left")
        .join(vel, on="customer_id", how="left")
        .join(merch, on="customer_id", how="left")
    )

    ontime_pct = F.when(
        F.coalesce(F.col("total_payments"), F.lit(0)) > 0,
        (F.col("ontime_payments") * F.lit(100.0) / F.col("total_payments")).cast(
            "decimal(5,2)"
        ),
    ).otherwise(F.lit(100.00).cast("decimal(5,2)"))

    final = result.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.coalesce(F.col("overdraft_count"), F.lit(0))
        .cast("int")
        .alias("account_overdraft_cnt"),
        F.coalesce(F.col("nsf_total"), F.lit(0.00))
        .cast("decimal(15,2)")
        .alias("nsf_fee_total"),
        F.coalesce(F.col("large_wd_cnt"), F.lit(0))
        .cast("int")
        .alias("large_withdrawal_cnt"),
        F.coalesce(F.col("large_wd_amt"), F.lit(0.00))
        .cast("decimal(18,2)")
        .alias("large_withdrawal_amt"),
        F.coalesce(F.col("avg_bal_30d"), F.lit(0.00))
        .cast("decimal(15,2)")
        .alias("avg_daily_balance_30d"),
        F.coalesce(F.col("avg_bal_90d"), F.lit(0.00))
        .cast("decimal(15,2)")
        .alias("avg_daily_balance_90d"),
        F.coalesce(F.col("bal_stddev"), F.lit(0.0000))
        .cast("decimal(10,4)")
        .alias("balance_volatility"),
        F.coalesce(F.col("credit_util_ratio"), F.lit(0.0000).cast("decimal(5,4)"))
        .cast("decimal(5,4)")
        .alias("credit_util_ratio"),
        ontime_pct.alias("payment_ontime_pct"),
        F.coalesce(F.col("late_payments"), F.lit(0))
        .cast("int")
        .alias("payment_late_cnt"),
        F.coalesce(F.col("months_since_last_late"), F.lit(999))
        .cast("int")
        .alias("months_since_last_late"),
        F.coalesce(F.col("credit_score"), F.lit(0))
        .cast("int")
        .alias("external_credit_score"),
        F.coalesce(F.col("debit_7d"), F.lit(0.00))
        .cast("decimal(15,2)")
        .alias("debit_velocity_7d"),
        F.coalesce(F.col("debit_30d"), F.lit(0.00))
        .cast("decimal(15,2)")
        .alias("debit_velocity_30d"),
        F.coalesce(F.col("new_merch_30d"), F.lit(0))
        .cast("int")
        .alias("new_merchant_cnt_30d"),
        F.coalesce(F.col("intl_txn_cnt"), F.lit(0))
        .cast("int")
        .alias("international_txn_cnt"),
        F.coalesce(F.col("high_risk_cnt"), F.lit(0))
        .cast("int")
        .alias("high_risk_merchant_cnt"),
        F.current_timestamp().alias("load_ts"),
    )

    return final


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    """Build and idempotently write ``etl_staging.stg_risk_factors`` (Delta)."""
    run_id = str(uuid.uuid4())
    target = cfg.table(cfg.schema_stg, "stg_risk_factors")

    init_audit(spark, cfg)
    log_step(spark, cfg, run_id, JOB_NAME, "FULL_LOAD", "START")

    try:
        result = build_risk_factors(spark, cfg)
        validate_table(
            result,
            min_rows=1,
            not_null_cols=["customer_id"],
            unique_keys=["customer_id"],
        )

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
        result.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)

        written = spark.table(target)
        row_count = written.count()
        log_step(
            spark,
            cfg,
            run_id,
            JOB_NAME,
            "FULL_LOAD",
            "SUCCESS",
            row_count=row_count,
            message=f"wrote {target}",
        )
        return written
    except Exception as exc:  # noqa: BLE001 - re-raised after audit
        log_step(
            spark,
            cfg,
            run_id,
            JOB_NAME,
            "FULL_LOAD",
            "ERROR",
            message=str(exc),
        )
        raise


__all__ = ["run", "build_risk_factors", "JOB_NAME"]
