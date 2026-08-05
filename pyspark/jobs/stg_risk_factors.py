"""PySpark port of ``bteq/03_stg_risk_factors.bteq``.

Builds ``ETL_STAGING_DB.STG_RISK_FACTORS``: one wide risk-factor row per active or inactive
customer, combining balance behaviour, payment history, transaction velocity, merchant risk and
external credit bureau data.

The BTEQ script materialises two work tables (``ETL_STAGING_DB.WRK_DAILY_BALANCE`` and
``ETL_STAGING_DB.WRK_PAYMENT_HISTORY``) and drops them again at the end of the run; neither has a
DDL contract. They are ported as intermediate DataFrames produced by :func:`transform_daily_balance`
and :func:`transform_payment_history` rather than as tables written through the IO layer, so the
"create ... drop" lifecycle disappears and the work sets stay unit-testable in isolation.

Every window boundary is derived from the pinned run date (``CURRENT_DATE`` in the legacy source);
each aggregate reads only the slice of ``TRANSACTIONS`` its own window needs.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.functions import (
    qualify_row_number,
    run_date_col,
    td_add_months,
    td_months_between,
)
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import TableSpec, enforce_schema
from common.validation import abort_on_failure, validate_table

JOB_NAME = "03_stg_risk_factors"
STEP_NAME = "FULL_LOAD"
TARGET = schemas.STG_RISK_FACTORS

#: ``WHERE c.CUSTOMER_STATUS IN ('A', 'I')`` - bteq line 305.
ACTIVE_CUSTOMER_STATUSES = ("A", "I")
#: ``t.STATUS_CODE = 'P'`` - bteq lines 41, 102, 185, 202, 270, 300.
POSTED_STATUS = "P"

#: ``ADD_MONTHS(CURRENT_DATE, -3)`` for WRK_DAILY_BALANCE - bteq line 40.
BALANCE_LOOKBACK_MONTHS = 3
#: ``ADD_MONTHS(CURRENT_DATE, -24)`` for WRK_PAYMENT_HISTORY - bteq line 103.
PAYMENT_LOOKBACK_MONTHS = 24
#: ``ADD_MONTHS(CURRENT_DATE, -12)`` for overdraft/NSF and large withdrawals - bteq lines 184, 201.
#: Numerically equal to ``config.lookback_months``; kept as the literal the BTEQ carries.
OVERDRAFT_LOOKBACK_MONTHS = 12
#: ``ADD_MONTHS(CURRENT_DATE, -6)`` for the merchant risk subquery - bteq line 299.
MERCHANT_LOOKBACK_MONTHS = 6

#: ``CURRENT_DATE - 30`` / ``CURRENT_DATE - 90`` average daily balance windows - bteq lines 211-212.
AVG_BALANCE_30D_DAYS = 30
AVG_BALANCE_90D_DAYS = 90
#: ``CURRENT_DATE - 7`` / ``CURRENT_DATE - 30`` debit velocity windows - bteq lines 261, 263, 269.
VELOCITY_7D_DAYS = 7
VELOCITY_30D_DAYS = 30
#: ``CURRENT_DATE - 30`` new-merchant window and its "seen before" cut-off - bteq lines 281, 286.
NEW_MERCHANT_WINDOW_DAYS = 30

#: ``ABS(t.AMOUNT) >= 5000`` - bteq line 200.
LARGE_WITHDRAWAL_THRESHOLD = 5000
#: ``tt.CATEGORY = 'FEE' AND tt.DESCRIPTION LIKE '%NSF%'`` - bteq line 179.
FEE_CATEGORY = "FEE"
NSF_DESCRIPTION_PATTERN = "%NSF%"
#: ``tt.CATEGORY = 'DEBIT'`` - bteq lines 199, 268.
DEBIT_CATEGORY = "DEBIT"
#: ``tt.CATEGORY = 'CREDIT'`` (payment transactions) - bteq line 101.
CREDIT_CATEGORY = "CREDIT"
#: ``acct.ACCOUNT_TYPE IN ('CREDIT', 'LOAN')`` - bteq line 100.
PAYMENT_ACCOUNT_TYPES = ("CREDIT", "LOAN")
#: ``ACCOUNT_TYPE = 'CREDIT' AND ACCOUNT_STATUS = 'O'`` - bteq lines 226-227.
CREDIT_ACCOUNT_TYPE = "CREDIT"
OPEN_ACCOUNT_STATUS = "O"
#: ``t.CHANNEL_CODE = 'INTL'`` - bteq line 292.
INTERNATIONAL_CHANNEL_CODE = "INTL"
#: ``t.MERCHANT_CATEGORY IN (...)`` - bteq lines 294-296.
HIGH_RISK_MERCHANT_CATEGORIES = ("GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP")

#: ``ELSE 100.00`` when a customer has no scheduled payments - bteq line 152.
DEFAULT_PAYMENT_ONTIME_PCT = 100.00
#: ``COALESCE(pmh.MONTHS_SINCE_LAST_LATE, 999)`` - bteq line 155.
DEFAULT_MONTHS_SINCE_LAST_LATE = 999
#: ``COALESCE(bureau.CREDIT_SCORE, 0)`` - bteq line 158. The 680 imputation happens downstream in
#: ``sas/03_sas_risk_scoring.sas``, never here.
DEFAULT_EXTERNAL_CREDIT_SCORE = 0


def _zero_like(column: Column, spec: TableSpec, name: str) -> Column:
    """``COALESCE(x, 0)`` with the zero typed as the DDL type of ``spec.name``.

    ``common.functions.zero_if_null`` coalesces against a *double* literal, which would widen the
    DECIMAL money columns to double and make the downstream sums and ratios inexact; the DDL type
    is used here instead so the arithmetic stays in DECIMAL as it is in Teradata.
    """

    return F.coalesce(column, F.lit(0).cast(spec.column(name).dtype))


def _zero(column: Column, target_column: str) -> Column:
    """``COALESCE(x, 0)`` for a target column, aliased to it."""

    return _zero_like(column, TARGET, target_column).alias(target_column)


def _posted_since(transactions: DataFrame, boundary: Column) -> DataFrame:
    """``WHERE t.TRANSACTION_DATE >= <boundary> AND t.STATUS_CODE = 'P'``."""

    return transactions.filter(
        (F.col("TRANSACTION_DATE") >= boundary) & (F.col("STATUS_CODE") == POSTED_STATUS)
    )


def _with_customer(transactions: DataFrame, accounts: DataFrame) -> DataFrame:
    """``INNER JOIN CORE_BANKING_DB.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID``."""

    return transactions.join(accounts.select("ACCOUNT_ID", "CUSTOMER_ID"), on="ACCOUNT_ID")


def _with_type(transactions: DataFrame, transaction_types: DataFrame) -> DataFrame:
    """``INNER JOIN TXN_PROCESSING_DB.TRANSACTION_TYPES tt ON t.TRANSACTION_TYPE_CD = ...``.

    ``TRANSACTION_TYPES`` is a handful of rows, so it is broadcast.
    """

    return transactions.join(
        F.broadcast(transaction_types.select("TRANSACTION_TYPE_CD", "DESCRIPTION", "CATEGORY")),
        on="TRANSACTION_TYPE_CD",
    )


def transform_daily_balance(
    transactions: DataFrame, accounts: DataFrame, *, run_date: date
) -> DataFrame:
    """``ETL_STAGING_DB.WRK_DAILY_BALANCE`` - closing balance per account and day (bteq 26-49).

    The last posted transaction of each account-day over the last three months supplies the
    end-of-day balance. ``QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID, TRANSACTION_DATE
    ORDER BY TRANSACTION_TS DESC) = 1`` is ambiguous when two transactions of the same account
    share a timestamp, so ``TRANSACTION_ID DESC`` is appended as a deterministic tiebreaker
    (highest transaction id wins, i.e. the last one written).
    """

    posted = _posted_since(
        transactions, td_add_months(run_date_col(run_date), -BALANCE_LOOKBACK_MONTHS)
    )
    closing = qualify_row_number(
        _with_customer(posted, accounts),
        partition_by=("ACCOUNT_ID", "TRANSACTION_DATE"),
        order_by=(F.col("TRANSACTION_TS").desc(), F.col("TRANSACTION_ID").desc()),
    )
    return closing.select(
        "CUSTOMER_ID",
        "ACCOUNT_ID",
        "TRANSACTION_DATE",
        F.col("RUNNING_BALANCE").alias("EOD_BALANCE"),
    )


def transform_payment_history(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    *,
    run_date: date,
) -> DataFrame:
    """``ETL_STAGING_DB.WRK_PAYMENT_HISTORY`` - payment behaviour per account (bteq 63-109).

    Legacy quirk preserved verbatim: the "on-time" test compares the transaction date against
    ``ADD_MONTHS(OPEN_DATE, CAST(MONTHS_BETWEEN(txn_date, OPEN_DATE) AS INTEGER) + 1)``, which is
    true for essentially every payment because the right-hand side is the transaction's own month
    shifted one month forward. ``ONTIME_PAYMENTS`` therefore equals ``TOTAL_PAYMENTS`` and
    ``LATE_PAYMENTS`` is 0 in practice (LEGACY_INVENTORY.md 5.9). It is ported as written.

    ``MONTHS_SINCE_LAST_LATE`` consequently falls back to its ``COALESCE(..., OPEN_DATE)`` branch:
    months between the run date and the account open date, truncated by ``CAST(... AS INTEGER)``.
    """

    payments = _posted_since(
        transactions, td_add_months(run_date_col(run_date), -PAYMENT_LOOKBACK_MONTHS)
    )
    typed = _with_type(payments, transaction_types).filter(F.col("CATEGORY") == CREDIT_CATEGORY)
    joined = typed.join(
        accounts.select("ACCOUNT_ID", "CUSTOMER_ID", "ACCOUNT_TYPE", "OPEN_DATE"),
        on="ACCOUNT_ID",
    ).filter(F.col("ACCOUNT_TYPE").isin(*PAYMENT_ACCOUNT_TYPES))

    due_date_proxy = F.add_months(
        F.col("OPEN_DATE"),
        td_months_between(F.col("TRANSACTION_DATE"), F.col("OPEN_DATE")) + F.lit(1),
    )
    ontime = F.col("TRANSACTION_DATE") <= due_date_proxy
    late = F.col("TRANSACTION_DATE") > due_date_proxy

    aggregated = joined.groupBy("CUSTOMER_ID", "ACCOUNT_ID").agg(
        F.count(F.lit(1)).alias("TOTAL_PAYMENTS"),
        F.sum(F.when(ontime, 1).otherwise(0)).alias("ONTIME_PAYMENTS"),
        F.sum(F.when(late, 1).otherwise(0)).alias("LATE_PAYMENTS"),
        F.max(F.when(late, F.col("TRANSACTION_DATE"))).alias("LAST_LATE_DATE"),
        # OPEN_DATE is functionally dependent on the grouped ACCOUNT_ID.
        F.max(F.col("OPEN_DATE")).alias("OPEN_DATE"),
    )
    return aggregated.select(
        "CUSTOMER_ID",
        "ACCOUNT_ID",
        "TOTAL_PAYMENTS",
        "ONTIME_PAYMENTS",
        "LATE_PAYMENTS",
        td_months_between(
            run_date_col(run_date), F.coalesce(F.col("LAST_LATE_DATE"), F.col("OPEN_DATE"))
        ).alias("MONTHS_SINCE_LAST_LATE"),
    )


def transform_overdraft_nsf(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    *,
    run_date: date,
) -> DataFrame:
    """Overdraft count and NSF fee total over the last 12 months (bteq 175-188).

    Legacy quirk preserved verbatim: ``OVERDRAFT_COUNT`` counts *transactions* whose running
    balance is negative, not distinct overdraft events, and it is not restricted to any account
    type (LEGACY_INVENTORY.md 5.8).
    """

    posted = _posted_since(
        transactions, td_add_months(run_date_col(run_date), -OVERDRAFT_LOOKBACK_MONTHS)
    )
    joined = _with_customer(_with_type(posted, transaction_types), accounts)
    is_nsf_fee = (F.col("CATEGORY") == FEE_CATEGORY) & F.col("DESCRIPTION").like(
        NSF_DESCRIPTION_PATTERN
    )
    return joined.groupBy("CUSTOMER_ID").agg(
        F.sum(F.when(F.col("RUNNING_BALANCE") < 0, 1).otherwise(0)).alias("OVERDRAFT_COUNT"),
        F.sum(F.when(is_nsf_fee, F.abs(F.col("AMOUNT"))).otherwise(F.lit(0))).alias("NSF_TOTAL"),
    )


def transform_large_withdrawals(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    *,
    run_date: date,
) -> DataFrame:
    """Single debits of at least $5,000 over the last 12 months (bteq 191-205)."""

    posted = _posted_since(
        transactions, td_add_months(run_date_col(run_date), -OVERDRAFT_LOOKBACK_MONTHS)
    )
    typed = _with_type(posted, transaction_types).filter(
        (F.col("CATEGORY") == DEBIT_CATEGORY)
        & (F.abs(F.col("AMOUNT")) >= F.lit(LARGE_WITHDRAWAL_THRESHOLD))
    )
    return (
        _with_customer(typed, accounts)
        .groupBy("CUSTOMER_ID")
        .agg(
            F.count(F.lit(1)).alias("LARGE_WD_CNT"),
            F.sum(F.abs(F.col("AMOUNT"))).alias("LARGE_WD_AMT"),
        )
    )


def transform_balance_metrics(daily_balance: DataFrame, *, run_date: date) -> DataFrame:
    """Average daily balance and volatility per customer (bteq 208-217).

    ``BAL_STDDEV`` is computed over the whole three-month work set while ``AVG_BAL_30D`` and
    ``AVG_BAL_90D`` re-filter that same set with ``CURRENT_DATE - 30`` / ``- 90``; the ``CASE``
    inside ``AVG`` yields NULL outside the window and SQL ``AVG`` ignores NULLs, so a customer
    with no balance row inside the window gets NULL (then coalesced to 0.00 downstream).
    """

    today = run_date_col(run_date)

    def average_since(days: int) -> Column:
        return F.avg(
            F.when(
                F.col("TRANSACTION_DATE") >= F.date_sub(today, days),
                F.col("EOD_BALANCE"),
            )
        )

    return daily_balance.groupBy("CUSTOMER_ID").agg(
        average_since(AVG_BALANCE_30D_DAYS).alias("AVG_BAL_30D"),
        average_since(AVG_BALANCE_90D_DAYS).alias("AVG_BAL_90D"),
        F.stddev_pop(F.col("EOD_BALANCE")).alias("BAL_STDDEV"),
    )


def transform_credit_exposure(accounts: DataFrame) -> DataFrame:
    """Open credit-card balance and limit per customer (bteq 220-230)."""

    credit_accounts = accounts.filter(
        (F.col("ACCOUNT_TYPE") == CREDIT_ACCOUNT_TYPE)
        & (F.col("ACCOUNT_STATUS") == OPEN_ACCOUNT_STATUS)
    )
    return credit_accounts.groupBy("CUSTOMER_ID").agg(
        F.sum(_zero_like(F.col("CURRENT_BALANCE"), schemas.ACCOUNTS, "CURRENT_BALANCE")).alias(
            "TOTAL_CREDIT_BAL"
        ),
        F.sum(_zero_like(F.col("CREDIT_LIMIT"), schemas.ACCOUNTS, "CREDIT_LIMIT")).alias(
            "TOTAL_CREDIT_LIMIT"
        ),
    )


def transform_payment_summary(payment_history: DataFrame) -> DataFrame:
    """Roll ``WRK_PAYMENT_HISTORY`` up from account level to customer level (bteq 233-243)."""

    return payment_history.groupBy("CUSTOMER_ID").agg(
        F.sum(F.col("TOTAL_PAYMENTS")).alias("TOTAL_PAYMENTS"),
        F.sum(F.col("ONTIME_PAYMENTS")).alias("ONTIME_PAYMENTS"),
        F.sum(F.col("LATE_PAYMENTS")).alias("LATE_PAYMENTS"),
        F.min(F.col("MONTHS_SINCE_LAST_LATE")).alias("MONTHS_SINCE_LAST_LATE"),
    )


def transform_latest_bureau_score(bureau_scores: DataFrame) -> DataFrame:
    """Latest external bureau report per customer (bteq 246-255).

    ``QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY REPORT_DATE DESC) = 1`` is
    ambiguous when a customer has two reports on the same date; ``EXTERNAL_CREDIT_SCORE DESC`` is
    appended as a deterministic tiebreaker (the only other column of the table).
    """

    latest = qualify_row_number(
        bureau_scores,
        partition_by=("CUSTOMER_ID",),
        order_by=(F.col("REPORT_DATE").desc(), F.col("EXTERNAL_CREDIT_SCORE").desc()),
    )
    return latest.select("CUSTOMER_ID", F.col("EXTERNAL_CREDIT_SCORE").alias("CREDIT_SCORE"))


def transform_debit_velocity(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    *,
    run_date: date,
) -> DataFrame:
    """Rolling 7-day and 30-day debit totals per customer (bteq 258-273).

    Only the 30-day slice is read; the 7-day total is a ``CASE`` inside the same aggregate.
    """

    today = run_date_col(run_date)
    posted = _posted_since(transactions, F.date_sub(today, VELOCITY_30D_DAYS))
    typed = _with_type(posted, transaction_types).filter(F.col("CATEGORY") == DEBIT_CATEGORY)

    def debit_since(days: int) -> Column:
        return F.sum(
            F.when(
                F.col("TRANSACTION_DATE") >= F.date_sub(today, days), F.abs(F.col("AMOUNT"))
            ).otherwise(F.lit(0))
        )

    return (
        _with_customer(typed, accounts)
        .groupBy("CUSTOMER_ID")
        .agg(
            debit_since(VELOCITY_7D_DAYS).alias("DEBIT_7D"),
            debit_since(VELOCITY_30D_DAYS).alias("DEBIT_30D"),
        )
    )


def transform_merchants_seen_before(transactions: DataFrame, *, run_date: date) -> DataFrame:
    """The per-account set of merchants used before the 30-day cut-off (bteq 283-288).

    This is the rewritten form of the correlated
    ``NOT IN (SELECT DISTINCT t2.MERCHANT_NAME ... WHERE t2.ACCOUNT_ID = t.ACCOUNT_ID AND
    t2.TRANSACTION_DATE < CURRENT_DATE - 30 AND t2.MERCHANT_NAME IS NOT NULL)`` subquery: the
    "seen before" set is built once per account and then used through a ``left_anti`` join.

    Two details of the legacy subquery are preserved: it applies **no** status filter and **no**
    lower date bound (it scans the account's whole history before the cut-off, unlike the
    six-month outer query), and it excludes NULL merchant names.
    """

    cutoff = F.date_sub(run_date_col(run_date), NEW_MERCHANT_WINDOW_DAYS)
    return (
        transactions.filter(
            (F.col("TRANSACTION_DATE") < cutoff) & F.col("MERCHANT_NAME").isNotNull()
        )
        .select("ACCOUNT_ID", "MERCHANT_NAME")
        .distinct()
    )


def transform_merchant_risk(
    transactions: DataFrame, accounts: DataFrame, *, run_date: date
) -> DataFrame:
    """New-merchant, international and high-risk merchant counts (bteq 276-303).

    Unlike its neighbouring subqueries this one does **not** join ``TRANSACTION_TYPES``, so no
    category filter applies; that is preserved.

    The "seen before" set is keyed per **account** while the aggregate is grouped per
    **customer**, so a merchant that is new to one of the customer's accounts counts even when
    another of their accounts used it before the cut-off. ``COUNT(DISTINCT ...)`` then
    de-duplicates the merchant names across the customer's accounts. NULL merchant names never
    contribute: the legacy ``NOT IN`` yields UNKNOWN for them and ``COUNT(DISTINCT)`` ignores the
    NULL the ``CASE`` produces - the ``left_anti`` join keeps such rows but ``count_distinct``
    ignores them identically.
    """

    today = run_date_col(run_date)
    recent_cutoff = F.date_sub(today, NEW_MERCHANT_WINDOW_DAYS)
    window = _with_customer(
        _posted_since(transactions, td_add_months(today, -MERCHANT_LOOKBACK_MONTHS)), accounts
    )

    counts = window.groupBy("CUSTOMER_ID").agg(
        F.sum(F.when(F.col("CHANNEL_CODE") == INTERNATIONAL_CHANNEL_CODE, 1).otherwise(0)).alias(
            "INTL_TXN_CNT"
        ),
        F.sum(
            F.when(F.col("MERCHANT_CATEGORY").isin(*HIGH_RISK_MERCHANT_CATEGORIES), 1).otherwise(0)
        ).alias("HIGH_RISK_CNT"),
    )

    new_merchants = (
        window.filter(F.col("TRANSACTION_DATE") >= recent_cutoff)
        .join(
            transform_merchants_seen_before(transactions, run_date=run_date),
            on=["ACCOUNT_ID", "MERCHANT_NAME"],
            how="left_anti",
        )
        .groupBy("CUSTOMER_ID")
        .agg(F.count_distinct(F.col("MERCHANT_NAME")).alias("NEW_MERCH_30D"))
    )

    return counts.join(new_merchants, on="CUSTOMER_ID", how="left")


def transform_risk_factors(
    customers: DataFrame,
    accounts: DataFrame,
    bureau_scores: DataFrame,
    transactions: DataFrame,
    transaction_types: DataFrame,
    *,
    run_date: date,
    load_ts: Column | None = None,
) -> DataFrame:
    """Full port of the ``CREATE MULTISET TABLE ETL_STAGING_DB.STG_RISK_FACTORS`` statement."""

    load_ts = F.current_timestamp() if load_ts is None else load_ts

    daily_balance = transform_daily_balance(transactions, accounts, run_date=run_date)
    payment_history = transform_payment_history(
        transactions, accounts, transaction_types, run_date=run_date
    )

    overdraft = transform_overdraft_nsf(
        transactions, accounts, transaction_types, run_date=run_date
    )
    large_withdrawals = transform_large_withdrawals(
        transactions, accounts, transaction_types, run_date=run_date
    )
    balances = transform_balance_metrics(daily_balance, run_date=run_date)
    credit = transform_credit_exposure(accounts)
    payments = transform_payment_summary(payment_history)
    bureau = transform_latest_bureau_score(bureau_scores)
    velocity = transform_debit_velocity(
        transactions, accounts, transaction_types, run_date=run_date
    )
    merchants = transform_merchant_risk(transactions, accounts, run_date=run_date)

    joined = customers.filter(F.col("CUSTOMER_STATUS").isin(*ACTIVE_CUSTOMER_STATUSES)).select(
        "CUSTOMER_ID"
    )
    for aggregate in (
        overdraft,
        large_withdrawals,
        balances,
        credit,
        payments,
        bureau,
        velocity,
        merchants,
    ):
        joined = joined.join(aggregate, on="CUSTOMER_ID", how="left")

    credit_util_ratio = F.when(
        F.col("TOTAL_CREDIT_LIMIT") > 0,
        (F.col("TOTAL_CREDIT_BAL") / F.col("TOTAL_CREDIT_LIMIT")).cast("decimal(5,4)"),
    ).otherwise(F.lit(0.0000).cast("decimal(5,4)"))

    payment_ontime_pct = F.when(
        F.col("TOTAL_PAYMENTS") > 0,
        (F.col("ONTIME_PAYMENTS") * F.lit(100.0) / F.col("TOTAL_PAYMENTS")).cast("decimal(5,2)"),
    ).otherwise(F.lit(DEFAULT_PAYMENT_ONTIME_PCT).cast("decimal(5,2)"))

    projected = joined.select(
        F.col("CUSTOMER_ID"),
        _zero(F.col("OVERDRAFT_COUNT"), "ACCOUNT_OVERDRAFT_CNT"),
        _zero(F.col("NSF_TOTAL"), "NSF_FEE_TOTAL"),
        _zero(F.col("LARGE_WD_CNT"), "LARGE_WITHDRAWAL_CNT"),
        _zero(F.col("LARGE_WD_AMT"), "LARGE_WITHDRAWAL_AMT"),
        _zero(F.col("AVG_BAL_30D"), "AVG_DAILY_BALANCE_30D"),
        _zero(F.col("AVG_BAL_90D"), "AVG_DAILY_BALANCE_90D"),
        _zero(F.col("BAL_STDDEV"), "BALANCE_VOLATILITY"),
        credit_util_ratio.alias("CREDIT_UTIL_RATIO"),
        payment_ontime_pct.alias("PAYMENT_ONTIME_PCT"),
        _zero(F.col("LATE_PAYMENTS"), "PAYMENT_LATE_CNT"),
        F.coalesce(F.col("MONTHS_SINCE_LAST_LATE"), F.lit(DEFAULT_MONTHS_SINCE_LAST_LATE)).alias(
            "MONTHS_SINCE_LAST_LATE"
        ),
        F.coalesce(F.col("CREDIT_SCORE"), F.lit(DEFAULT_EXTERNAL_CREDIT_SCORE)).alias(
            "EXTERNAL_CREDIT_SCORE"
        ),
        _zero(F.col("DEBIT_7D"), "DEBIT_VELOCITY_7D"),
        _zero(F.col("DEBIT_30D"), "DEBIT_VELOCITY_30D"),
        _zero(F.col("NEW_MERCH_30D"), "NEW_MERCHANT_CNT_30D"),
        _zero(F.col("INTL_TXN_CNT"), "INTERNATIONAL_TXN_CNT"),
        _zero(F.col("HIGH_RISK_CNT"), "HIGH_RISK_MERCHANT_CNT"),
        load_ts.alias("LOAD_TS"),
    )
    return enforce_schema(projected, TARGET)


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the BTEQ script's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    audit.log_step(JOB_NAME, "START", "Building STG_RISK_FACTORS")

    customers = io.read_spec(schemas.CUSTOMERS)
    accounts = io.read_spec(schemas.ACCOUNTS)
    bureau_scores = io.read_spec(schemas.CUSTOMER_BUREAU_SCORES)
    transactions = io.read_spec(schemas.TRANSACTIONS)
    transaction_types = io.read_spec(schemas.TRANSACTION_TYPES)

    # The two BTEQ work tables are intermediate DataFrames here; both are consumed twice
    # (once by their aggregate, once by the transaction scan that feeds it), so they are cached.
    audit.log_step(JOB_NAME, "START", "WRK_DAILY_BALANCE + WRK_PAYMENT_HISTORY (in-memory)")

    output = transform_risk_factors(
        customers,
        accounts,
        bureau_scores,
        transactions,
        transaction_types,
        run_date=config.run_date,
    ).persist()

    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=TARGET.primary_index,
        not_null=("CUSTOMER_ID",),
        min_rows=1,
    )
    result.validation = validation
    abort_on_failure(validation)

    # COLLECT STATISTICS in the BTEQ script; Spark maintains its own statistics.
    audit.log_step(JOB_NAME, "START", "COLLECT STATISTICS (no-op on Spark)")

    row_count = io.write_spec(output, TARGET)
    output.unpersist()

    result.row_count = row_count
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "STG_RISK_FACTORS loaded", rowcount=row_count)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", row_count, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
