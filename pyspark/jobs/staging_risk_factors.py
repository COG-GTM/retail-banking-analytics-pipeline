"""Staging job 03 -- STG_RISK_FACTORS.

Faithful PySpark port of ``bteq/03_stg_risk_factors.bteq``: a per-customer risk
feature vector assembled from balance behaviour, overdraft/NSF activity, large
withdrawals, credit utilisation, payment history, external bureau scores,
transaction velocity and merchant-risk indicators.

The BTEQ script is a base scan of ``CORE_BANKING_DB.CUSTOMERS`` (status ``A``/``I``)
LEFT JOINed to many per-customer sub-selects, each ``COALESCE``d to a default.
Every sub-select is ported here as its own pure, unit-testable helper; the thin
:func:`run` wires I/O, validation, audit and the schema contract.

BTEQ -> PySpark mapping (see PR body for the full table):
* ``ADD_MONTHS(CURRENT_DATE, -n)`` -> :func:`common.config.add_months` on
  ``config.run_date`` (never the wall clock).
* ``CURRENT_DATE - n`` (day arithmetic) -> ``run_date - timedelta(days=n)``.
* ``QUALIFY ROW_NUMBER() OVER (...) = 1`` -> :class:`Window` + ``row_number``.
* ``CAST(x AS INTEGER)`` -> ``.cast("int")`` (Spark truncates toward zero, matching
  Teradata's ``(INTEGER)`` cast).
* ``STDDEV_POP`` -> :func:`pyspark.sql.functions.stddev_pop`.
* The small ``TRANSACTION_TYPES`` dimension is ``F.broadcast``-joined everywhere.
* The correlated ``NOT IN`` new-merchant sub-query is rewritten as a windowed
  first-seen + anti (``left_semi``) join -- see :func:`new_merchant_counts`.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig, add_months
from common.dates import load_timestamp
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table

JOB_NAME = "03_stg_risk_factors"
TARGET = "STG_RISK_FACTORS"

_ACTIVE_STATUSES = ("A", "I")
_HIGH_RISK_CATEGORIES = ("GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP")


def _with_customer(transactions: DataFrame, accounts: DataFrame) -> DataFrame:
    """Map ACCOUNT_ID -> CUSTOMER_ID (the ``INNER JOIN ACCOUNTS`` every sub-select uses)."""
    acct = accounts.select("account_id", "customer_id")
    return transactions.join(acct, "account_id", "inner")


def _join_types(df: DataFrame, transaction_types: DataFrame, *cols: str) -> DataFrame:
    """Broadcast-join the small TRANSACTION_TYPES dimension (Rules: broadcast dims)."""
    tt = transaction_types.select("transaction_type_cd", *cols)
    return df.join(F.broadcast(tt), "transaction_type_cd", "inner")


# --------------------------------------------------------------------------- #
# Balance behaviour                                                           #
# --------------------------------------------------------------------------- #
def wrk_daily_balance(transactions: DataFrame, accounts: DataFrame, run_date: _dt.date) -> DataFrame:
    """WRK_DAILY_BALANCE: last posted txn per (account, day) over the last 3 months.

    ``EOD_BALANCE`` is that day's running balance. Mirrors the ``QUALIFY
    ROW_NUMBER() OVER (PARTITION BY account_id, transaction_date ORDER BY
    transaction_ts DESC) = 1`` intermediate (``transaction_id`` added as a
    deterministic tiebreaker for equal timestamps).
    """
    start = add_months(run_date, -3)
    base = _with_customer(
        transactions.filter(
            (F.col("status_code") == "P") & (F.col("transaction_date") >= F.lit(start))
        ),
        accounts,
    )
    win = Window.partitionBy("account_id", "transaction_date").orderBy(
        F.col("transaction_ts").desc_nulls_last(), F.col("transaction_id").desc()
    )
    return (
        base.withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_id",
            "account_id",
            "transaction_date",
            F.col("running_balance").alias("eod_balance"),
        )
    )


def balance_metrics(daily_balance: DataFrame, run_date: _dt.date) -> DataFrame:
    """Per-customer 30d/90d average daily balance and 3-month balance volatility."""
    d30 = run_date - _dt.timedelta(days=30)
    d90 = run_date - _dt.timedelta(days=90)
    return daily_balance.groupBy("customer_id").agg(
        F.avg(F.when(F.col("transaction_date") >= F.lit(d30), F.col("eod_balance"))).alias(
            "avg_daily_balance_30d"
        ),
        F.avg(F.when(F.col("transaction_date") >= F.lit(d90), F.col("eod_balance"))).alias(
            "avg_daily_balance_90d"
        ),
        F.stddev_pop("eod_balance").alias("balance_volatility"),
    )


# --------------------------------------------------------------------------- #
# Overdraft / NSF and large withdrawals                                       #
# --------------------------------------------------------------------------- #
def overdraft_nsf(
    transactions: DataFrame, accounts: DataFrame, transaction_types: DataFrame, run_date: _dt.date
) -> DataFrame:
    """Overdraft occurrences and NSF fee total over the last 12 months."""
    start = add_months(run_date, -12)
    base = _join_types(
        _with_customer(
            transactions.filter(
                (F.col("status_code") == "P") & (F.col("transaction_date") >= F.lit(start))
            ),
            accounts,
        ),
        transaction_types,
        "category",
        "description",
    )
    return base.groupBy("customer_id").agg(
        F.sum(F.when(F.col("running_balance") < 0, 1).otherwise(0)).alias("account_overdraft_cnt"),
        F.sum(
            F.when(
                (F.col("category") == "FEE") & F.col("description").like("%NSF%"),
                F.abs(F.col("amount")),
            ).otherwise(F.lit(0))
        ).alias("nsf_fee_total"),
    )


def large_withdrawals(
    transactions: DataFrame, accounts: DataFrame, transaction_types: DataFrame, run_date: _dt.date
) -> DataFrame:
    """Count and total of single DEBIT withdrawals >= $5,000 over the last 12 months."""
    start = add_months(run_date, -12)
    base = _join_types(
        _with_customer(
            transactions.filter(
                (F.col("status_code") == "P")
                & (F.col("transaction_date") >= F.lit(start))
                & (F.abs(F.col("amount")) >= F.lit(5000))
            ),
            accounts,
        ),
        transaction_types,
        "category",
    ).filter(F.col("category") == "DEBIT")
    return base.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("large_withdrawal_cnt"),
        F.sum(F.abs(F.col("amount"))).alias("large_withdrawal_amt"),
    )


# --------------------------------------------------------------------------- #
# Credit utilisation                                                          #
# --------------------------------------------------------------------------- #
def credit_util(accounts: DataFrame) -> DataFrame:
    """CREDIT_UTIL_RATIO = SUM(balance) / SUM(limit) over open CREDIT accounts (0 if no limit)."""
    agg = (
        accounts.filter((F.col("account_type") == "CREDIT") & (F.col("account_status") == "O"))
        .groupBy("customer_id")
        .agg(
            F.sum(F.coalesce(F.col("current_balance"), F.lit(0))).alias("total_credit_bal"),
            F.sum(F.coalesce(F.col("credit_limit"), F.lit(0))).alias("total_credit_limit"),
        )
    )
    ratio = F.when(
        F.col("total_credit_limit") > 0,
        F.col("total_credit_bal") / F.col("total_credit_limit"),
    ).otherwise(F.lit(0.0))
    return agg.select("customer_id", ratio.alias("credit_util_ratio"))


# --------------------------------------------------------------------------- #
# Payment history                                                             #
# --------------------------------------------------------------------------- #
def wrk_payment_history(
    transactions: DataFrame, accounts: DataFrame, transaction_types: DataFrame, run_date: _dt.date
) -> DataFrame:
    """WRK_PAYMENT_HISTORY: per (customer, account) payment counts over 24 months.

    On-time/late uses the due-date proxy ``ADD_MONTHS(open_date,
    CAST(MONTHS_BETWEEN(txn_date, open_date) AS INT) + 1)``. ``MONTHS_SINCE_LAST_LATE``
    is months between ``run_date`` and the last late payment (or ``open_date`` when
    there was never a late payment).

    Fidelity note: this legacy proxy is degenerate. Since ``floor(mb) + 1 >= mb``,
    ``_due`` is always on/after ``transaction_date``, so no payment is ever flagged
    late -- ``payment_late_cnt`` is always 0 and ``months_since_last_late`` always
    derives from ``open_date``. This is preserved verbatim from the BTEQ (not a
    translation artifact); it is documented here and in the PR body.
    """
    start = add_months(run_date, -24)
    acct = accounts.select("account_id", "customer_id", "account_type", "open_date")
    base = (
        transactions.filter(
            (F.col("status_code") == "P") & (F.col("transaction_date") >= F.lit(start))
        )
        .join(acct, "account_id", "inner")
        .filter(F.col("account_type").isin("CREDIT", "LOAN"))
    )
    base = _join_types(base, transaction_types, "category").filter(F.col("category") == "CREDIT")

    due = F.expr(
        "ADD_MONTHS(open_date, CAST(MONTHS_BETWEEN(transaction_date, open_date) AS INT) + 1)"
    )
    base = base.withColumn("_due", due)
    return (
        base.groupBy("customer_id", "account_id", "open_date")
        .agg(
            F.count(F.lit(1)).alias("total_payments"),
            F.sum(F.when(F.col("transaction_date") <= F.col("_due"), 1).otherwise(0)).alias(
                "ontime_payments"
            ),
            F.sum(F.when(F.col("transaction_date") > F.col("_due"), 1).otherwise(0)).alias(
                "late_payments"
            ),
            F.max(F.when(F.col("transaction_date") > F.col("_due"), F.col("transaction_date"))).alias(
                "_max_late_date"
            ),
        )
        .withColumn(
            "months_since_last_late",
            F.months_between(
                F.lit(run_date), F.coalesce(F.col("_max_late_date"), F.col("open_date"))
            ).cast("int"),
        )
        .select(
            "customer_id",
            "account_id",
            "total_payments",
            "ontime_payments",
            "late_payments",
            "months_since_last_late",
        )
    )


def payment_history(payment_hist: DataFrame) -> DataFrame:
    """Aggregate WRK_PAYMENT_HISTORY to the customer grain (BTEQ sub-select ``pmh``)."""
    agg = payment_hist.groupBy("customer_id").agg(
        F.sum("total_payments").alias("total_payments"),
        F.sum("ontime_payments").alias("ontime_payments"),
        F.sum("late_payments").alias("late_payments"),
        F.min("months_since_last_late").alias("months_since_last_late"),
    )
    ontime_pct = F.when(
        F.col("total_payments") > 0,
        F.col("ontime_payments") * 100.0 / F.col("total_payments"),
    ).otherwise(F.lit(100.00))
    return agg.select(
        "customer_id",
        ontime_pct.alias("payment_ontime_pct"),
        F.col("late_payments").alias("payment_late_cnt"),
        F.col("months_since_last_late"),
    )


# --------------------------------------------------------------------------- #
# External bureau score                                                       #
# --------------------------------------------------------------------------- #
def bureau_scores(bureau: DataFrame) -> DataFrame:
    """Latest EXTERNAL_CREDIT_SCORE per customer (highest score breaks report_date ties)."""
    win = Window.partitionBy("customer_id").orderBy(
        F.col("report_date").desc_nulls_last(), F.col("external_credit_score").desc_nulls_last()
    )
    return (
        bureau.withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .select("customer_id", "external_credit_score")
    )


# --------------------------------------------------------------------------- #
# Transaction velocity                                                        #
# --------------------------------------------------------------------------- #
def debit_velocity(
    transactions: DataFrame, accounts: DataFrame, transaction_types: DataFrame, run_date: _dt.date
) -> DataFrame:
    """7-day and 30-day rolling DEBIT outflow totals."""
    d7 = run_date - _dt.timedelta(days=7)
    d30 = run_date - _dt.timedelta(days=30)
    base = _join_types(
        _with_customer(
            transactions.filter(
                (F.col("status_code") == "P") & (F.col("transaction_date") >= F.lit(d30))
            ),
            accounts,
        ),
        transaction_types,
        "category",
    ).filter(F.col("category") == "DEBIT")
    return base.groupBy("customer_id").agg(
        F.sum(
            F.when(F.col("transaction_date") >= F.lit(d7), F.abs(F.col("amount"))).otherwise(F.lit(0))
        ).alias("debit_velocity_7d"),
        F.sum(
            F.when(F.col("transaction_date") >= F.lit(d30), F.abs(F.col("amount"))).otherwise(F.lit(0))
        ).alias("debit_velocity_30d"),
    )


# --------------------------------------------------------------------------- #
# Merchant risk                                                               #
# --------------------------------------------------------------------------- #
def new_merchant_counts(
    transactions: DataFrame, accounts: DataFrame, run_date: _dt.date
) -> DataFrame:
    """New merchants (never used before ``run_date - 30``) transacted in the last 30 days.

    Rewrite of the legacy correlated ``NOT IN`` sub-query: ``first_seen`` is the
    earliest transaction date per ``(account_id, merchant_name)`` over all history;
    a merchant is "new" when ``first_seen >= run_date - 30`` (equivalent to "never
    used by that account before ``run_date - 30``"). A ``left_semi`` (anti) join keeps
    only qualifying posted transactions in the last 30 days, then distinct merchant
    names are counted per customer.
    """
    cutoff = run_date - _dt.timedelta(days=30)
    acct = accounts.select("account_id", "customer_id")
    first_seen = (
        transactions.filter(F.col("merchant_name").isNotNull())
        .groupBy("account_id", "merchant_name")
        .agg(F.min("transaction_date").alias("first_seen"))
    )
    new_pairs = first_seen.filter(F.col("first_seen") >= F.lit(cutoff)).select(
        "account_id", "merchant_name"
    )
    recent = (
        transactions.filter(
            (F.col("status_code") == "P")
            & (F.col("transaction_date") >= F.lit(cutoff))
            & F.col("merchant_name").isNotNull()
        )
        .join(acct, "account_id", "inner")
        .join(new_pairs, ["account_id", "merchant_name"], "left_semi")
    )
    return recent.groupBy("customer_id").agg(
        F.countDistinct("merchant_name").alias("new_merchant_cnt_30d")
    )


def merchant_activity(
    transactions: DataFrame, accounts: DataFrame, run_date: _dt.date
) -> DataFrame:
    """International and high-risk-merchant transaction counts over the last 6 months."""
    start = add_months(run_date, -6)
    base = _with_customer(
        transactions.filter(
            (F.col("status_code") == "P") & (F.col("transaction_date") >= F.lit(start))
        ),
        accounts,
    )
    return base.groupBy("customer_id").agg(
        F.sum(F.when(F.col("channel_code") == "INTL", 1).otherwise(0)).alias("international_txn_cnt"),
        F.sum(
            F.when(F.col("merchant_category").isin(*_HIGH_RISK_CATEGORIES), 1).otherwise(0)
        ).alias("high_risk_merchant_cnt"),
    )


# --------------------------------------------------------------------------- #
# Assembly                                                                     #
# --------------------------------------------------------------------------- #
def transform(
    customers: DataFrame,
    accounts: DataFrame,
    transactions: DataFrame,
    transaction_types: DataFrame,
    bureau: DataFrame,
    config: PipelineConfig,
) -> DataFrame:
    """Build STG_RISK_FACTORS (schema-enforced to the DDL)."""
    run_date = config.run_date

    bal = balance_metrics(wrk_daily_balance(transactions, accounts, run_date), run_date)
    od = overdraft_nsf(transactions, accounts, transaction_types, run_date)
    lg = large_withdrawals(transactions, accounts, transaction_types, run_date)
    cu = credit_util(accounts)
    pmh = payment_history(wrk_payment_history(transactions, accounts, transaction_types, run_date))
    br = bureau_scores(bureau)
    vel = debit_velocity(transactions, accounts, transaction_types, run_date)
    nm = new_merchant_counts(transactions, accounts, run_date)
    ma = merchant_activity(transactions, accounts, run_date)

    df = (
        customers.filter(F.col("customer_status").isin(*_ACTIVE_STATUSES))
        .select("customer_id")
        .join(od, "customer_id", "left")
        .join(lg, "customer_id", "left")
        .join(bal, "customer_id", "left")
        .join(cu, "customer_id", "left")
        .join(pmh, "customer_id", "left")
        .join(br, "customer_id", "left")
        .join(vel, "customer_id", "left")
        .join(nm, "customer_id", "left")
        .join(ma, "customer_id", "left")
    )

    # COALESCE every metric to its BTEQ default.
    defaults = {
        "account_overdraft_cnt": F.lit(0),
        "nsf_fee_total": F.lit(0.00),
        "large_withdrawal_cnt": F.lit(0),
        "large_withdrawal_amt": F.lit(0.00),
        "avg_daily_balance_30d": F.lit(0.00),
        "avg_daily_balance_90d": F.lit(0.00),
        "balance_volatility": F.lit(0.0000),
        "credit_util_ratio": F.lit(0.0000),
        "payment_ontime_pct": F.lit(100.00),
        "payment_late_cnt": F.lit(0),
        "months_since_last_late": F.lit(999),
        "external_credit_score": F.lit(0),
        "debit_velocity_7d": F.lit(0.00),
        "debit_velocity_30d": F.lit(0.00),
        "new_merchant_cnt_30d": F.lit(0),
        "international_txn_cnt": F.lit(0),
        "high_risk_merchant_cnt": F.lit(0),
    }
    for col, default in defaults.items():
        df = df.withColumn(col, F.coalesce(F.col(col), default))

    df = df.withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    return schemas.enforce_schema(df, schemas.STG_RISK_FACTORS)


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read sources, transform, validate, and write STG_RISK_FACTORS."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", "Beginning risk-factors staging")

    customers = io.read_source("CUSTOMERS")
    accounts = io.read_source("ACCOUNTS")
    transactions = io.read_source("TRANSACTIONS")
    transaction_types = io.read_source("TRANSACTION_TYPES")
    bureau = io.read_source("CUSTOMER_BUREAU_SCORES")

    out = transform(customers, accounts, transactions, transaction_types, bureau, config).cache()
    n = out.count()

    result = validate_table(
        out, TARGET, key_cols=["customer_id"], not_null=["customer_id"], min_rows=1, audit=audit,
    )
    abort_on_failure(result)

    io.write_staging(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Staging table written", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build STG_RISK_FACTORS")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(spark, config, args.source_dir, args.lake_dir)
    run(spark, io, config)


if __name__ == "__main__":
    main()
