"""PySpark port of ``bteq/02_stg_txn_summary.bteq``.

Builds ``ETL_STAGING_DB.STG_TXN_SUMMARY``: one summary row per account over the configured
lookback window, aggregating volume, dollar, diversity, channel-mix and recency metrics from
the posted transactions of that window.

Every aggregate is computed over the same subset: transactions with ``STATUS_CODE = 'P'`` whose
``TRANSACTION_DATE`` falls inside ``[PERIOD_START, PERIOD_END]``, inner-joined to
``CORE_BANKING_DB.ACCOUNTS`` and to ``TXN_PROCESSING_DB.TRANSACTION_TYPES`` (so a transaction on
an unknown account or with an unknown transaction type code drops out of every metric).
``TOP_MERCHANT_CATEGORY`` is the one exception: it comes from a subquery over the *same* posted
window slice but *before* those two inner joins, restricted to non-NULL merchant categories.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.functions import nullif_zero, qualify_row_number, run_date_col, td_add_months
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import enforce_schema
from common.validation import abort_on_failure, validate_table

JOB_NAME = "02_stg_txn_summary"
STEP_NAME = "FULL_LOAD"
TARGET = schemas.STG_TXN_SUMMARY

#: ``AND t.STATUS_CODE = 'P'   /* Posted transactions only */``
POSTED_STATUS = "P"
#: ``tt.CATEGORY`` values tested by the CASE expressions of the SELECT list.
CATEGORY_DEBIT = "DEBIT"
CATEGORY_CREDIT = "CREDIT"
CATEGORY_FEE = "FEE"
#: ``t.CHANNEL_CODE = 'ATM' | 'POS' | 'WEB'`` and ``t.CHANNEL_CODE IN ('MOB')``.
CHANNEL_ATM = "ATM"
CHANNEL_POS = "POS"
CHANNEL_WEB = "WEB"
CHANNEL_MOBILE = "MOB"
#: ``* 100.0 / NULLIFZERO(COUNT(*))`` in the channel-mix expressions.
PERCENT_MULTIPLIER = 100
#: ``CAST(... AS DECIMAL(5,2))`` around every channel percentage.
CHANNEL_PCT_TYPE = "decimal(5,2)"

_TOP_CATEGORY_COL = "_TOP_MERCHANT_CATEGORY"
_CATEGORY_SPEND_COL = "_CATEGORY_SPEND"


def run_params(run_date: date, lookback_months: int) -> tuple[Column, Column]:
    """Port of the ``VT_RUN_PARAMS`` volatile table and its ``CROSS JOIN``.

    ``ADD_MONTHS(CURRENT_DATE, -${LOOKBACK_MONTHS})`` and ``CURRENT_DATE`` are run-level
    constants, so the cross join collapses into two literal columns. ``LOOKBACK_MONTHS`` comes
    from ``config/pipeline_config.cfg`` via :class:`~common.config.PipelineConfig`.
    """

    period_end = run_date_col(run_date)
    period_start = td_add_months(period_end, -lookback_months)
    return period_start, period_end


def transform_posted_transactions(
    transactions: DataFrame, period_start: Column, period_end: Column
) -> DataFrame:
    """``WHERE t.TRANSACTION_DATE BETWEEN rp.PERIOD_START AND rp.PERIOD_END AND t.STATUS_CODE = 'P'``.

    Applied before any join so that only the window's date slice is read and shuffled.
    """

    return transactions.filter(
        (F.col("STATUS_CODE") == POSTED_STATUS)
        & F.col("TRANSACTION_DATE").between(period_start, period_end)
    )


def transform_top_merchant_category(posted_transactions: DataFrame) -> DataFrame:
    """Port of the ``top_cat`` subquery: the highest-spend merchant category per account.

    The legacy ``QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY SUM(ABS(AMOUNT))
    OVER (PARTITION BY ACCOUNT_ID, MERCHANT_CATEGORY) DESC) = 1`` is a rank over a windowed sum,
    so the spend is pre-aggregated per ``(ACCOUNT_ID, MERCHANT_CATEGORY)`` and ranked once.

    The legacy ordering is ambiguous when two categories tie on spend; ``MERCHANT_CATEGORY``
    ascending is appended as a deterministic tiebreaker.

    The subquery filters the same window and ``STATUS_CODE = 'P'`` as the outer query, but is
    *not* joined to ``ACCOUNTS`` or ``TRANSACTION_TYPES``, so spend on a transaction with an
    unknown type code still counts here.
    """

    spend = (
        posted_transactions.filter(F.col("MERCHANT_CATEGORY").isNotNull())
        .groupBy("ACCOUNT_ID", "MERCHANT_CATEGORY")
        .agg(F.sum(F.abs(F.col("AMOUNT"))).alias(_CATEGORY_SPEND_COL))
    )
    ranked = qualify_row_number(
        spend,
        partition_by=("ACCOUNT_ID",),
        order_by=(F.col(_CATEGORY_SPEND_COL).desc(), F.col("MERCHANT_CATEGORY").asc()),
    )
    return ranked.select("ACCOUNT_ID", F.col("MERCHANT_CATEGORY").alias(_TOP_CATEGORY_COL))


def transform_account_summary(
    posted_transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    top_categories: DataFrame,
    *,
    run_date: date,
) -> DataFrame:
    """Port of the ``GROUP BY`` block of the ``CREATE MULTISET TABLE`` statement.

    ``top_cat`` is joined on ``ACCOUNT_ID`` only - it carries no period predicate on the outer
    side - and ``TOP_MERCHANT_CATEGORY`` is both grouped by *and* wrapped in ``MAX()``. Both are
    preserved: the subquery yields at most one row per account, so grouping by it cannot split a
    group and the ``MAX()`` is a no-op, but the shape is kept verbatim.
    """

    today = run_date_col(run_date)
    categories = F.broadcast(
        transaction_types.select("TRANSACTION_TYPE_CD", "CATEGORY")
    )  # 8-row dimension

    joined = (
        posted_transactions.join(
            accounts.select("ACCOUNT_ID", "CUSTOMER_ID", "ACCOUNT_TYPE"), on="ACCOUNT_ID"
        )
        .join(categories, on="TRANSACTION_TYPE_CD")
        .join(top_categories, on="ACCOUNT_ID", how="left")
    )

    total_count = F.count(F.lit(1))
    amount = F.col("AMOUNT")

    def category_is(category: str) -> Column:
        return F.col("CATEGORY") == category

    def count_of(category: str) -> Column:
        return F.sum(F.when(category_is(category), F.lit(1)).otherwise(F.lit(0)))

    def sum_of(category: str, value: Column) -> Column:
        return F.sum(F.when(category_is(category), value).otherwise(F.lit(0)))

    def avg_of(category: str, value: Column) -> Column:
        return F.avg(F.when(category_is(category), value))

    def max_of(category: str, value: Column) -> Column:
        return F.max(F.when(category_is(category), value).otherwise(F.lit(0)))

    def channel_pct(channel: str) -> Column:
        channel_count = F.sum(
            F.when(F.col("CHANNEL_CODE") == channel, F.lit(1)).otherwise(F.lit(0))
        )
        numerator = (channel_count * F.lit(PERCENT_MULTIPLIER)).cast("decimal(20,0)")
        return (numerator / nullif_zero(total_count).cast("decimal(20,0)")).cast(CHANNEL_PCT_TYPE)

    return (
        joined.groupBy("CUSTOMER_ID", "ACCOUNT_ID", "ACCOUNT_TYPE", _TOP_CATEGORY_COL)
        .agg(
            total_count.alias("TXN_COUNT_TOTAL"),
            count_of(CATEGORY_DEBIT).alias("TXN_COUNT_DEBIT"),
            count_of(CATEGORY_CREDIT).alias("TXN_COUNT_CREDIT"),
            count_of(CATEGORY_FEE).alias("TXN_COUNT_FEE"),
            sum_of(CATEGORY_DEBIT, F.abs(amount)).alias("AMT_TOTAL_DEBIT"),
            sum_of(CATEGORY_CREDIT, amount).alias("AMT_TOTAL_CREDIT"),
            sum_of(CATEGORY_FEE, F.abs(amount)).alias("AMT_TOTAL_FEES"),
            avg_of(CATEGORY_DEBIT, F.abs(amount)).alias("AMT_AVG_DEBIT"),
            avg_of(CATEGORY_CREDIT, amount).alias("AMT_AVG_CREDIT"),
            max_of(CATEGORY_DEBIT, F.abs(amount)).alias("AMT_MAX_SINGLE_DEBIT"),
            max_of(CATEGORY_CREDIT, amount).alias("AMT_MAX_SINGLE_CREDIT"),
            F.count_distinct(F.col("MERCHANT_NAME")).alias("DISTINCT_MERCHANTS"),
            F.max(F.col(_TOP_CATEGORY_COL)).alias("TOP_MERCHANT_CATEGORY"),
            channel_pct(CHANNEL_ATM).alias("PCT_ATM"),
            channel_pct(CHANNEL_POS).alias("PCT_POS"),
            channel_pct(CHANNEL_WEB).alias("PCT_WEB"),
            channel_pct(CHANNEL_MOBILE).alias("PCT_MOBILE"),
            F.datediff(today, F.max(F.col("TRANSACTION_DATE"))).alias("DAYS_SINCE_LAST_TXN"),
        )
        .drop(_TOP_CATEGORY_COL)
    )


def transform_txn_summary(
    transactions: DataFrame,
    transaction_types: DataFrame,
    accounts: DataFrame,
    *,
    run_date: date,
    lookback_months: int,
    load_ts: Column | None = None,
) -> DataFrame:
    """Full port of the ``CREATE MULTISET TABLE ETL_STAGING_DB.STG_TXN_SUMMARY`` statement."""

    load_ts = F.current_timestamp() if load_ts is None else load_ts
    period_start, period_end = run_params(run_date, lookback_months)

    posted = transform_posted_transactions(transactions, period_start, period_end)
    top_categories = transform_top_merchant_category(posted)
    summary = transform_account_summary(
        posted, accounts, transaction_types, top_categories, run_date=run_date
    )

    projected = summary.select(
        F.col("CUSTOMER_ID"),
        F.col("ACCOUNT_ID"),
        F.col("ACCOUNT_TYPE"),
        period_start.alias("SUMMARY_PERIOD_START"),
        period_end.alias("SUMMARY_PERIOD_END"),
        F.col("TXN_COUNT_TOTAL"),
        F.col("TXN_COUNT_DEBIT"),
        F.col("TXN_COUNT_CREDIT"),
        F.col("TXN_COUNT_FEE"),
        F.col("AMT_TOTAL_DEBIT"),
        F.col("AMT_TOTAL_CREDIT"),
        F.col("AMT_TOTAL_FEES"),
        F.col("AMT_AVG_DEBIT"),
        F.col("AMT_AVG_CREDIT"),
        F.col("AMT_MAX_SINGLE_DEBIT"),
        F.col("AMT_MAX_SINGLE_CREDIT"),
        F.col("DISTINCT_MERCHANTS"),
        F.col("TOP_MERCHANT_CATEGORY"),
        F.col("PCT_ATM"),
        F.col("PCT_POS"),
        F.col("PCT_WEB"),
        F.col("PCT_MOBILE"),
        F.col("DAYS_SINCE_LAST_TXN"),
        load_ts.alias("LOAD_TS"),
    )
    return enforce_schema(projected, TARGET)


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the BTEQ script's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    audit.log_step(JOB_NAME, "START", "Building STG_TXN_SUMMARY")

    transactions = io.read_spec(schemas.TRANSACTIONS)
    transaction_types = io.read_spec(schemas.TRANSACTION_TYPES)
    accounts = io.read_spec(schemas.ACCOUNTS)

    output = transform_txn_summary(
        transactions,
        transaction_types,
        accounts,
        run_date=config.run_date,
        lookback_months=config.lookback_months,
    ).persist()

    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=TARGET.primary_index,
        not_null=("CUSTOMER_ID", "ACCOUNT_ID"),
        min_rows=1,
    )
    result.validation = validation
    abort_on_failure(validation)

    # Step 3 in the BTEQ script; Spark maintains its own statistics.
    audit.log_step(JOB_NAME, "START", "COLLECT STATISTICS (no-op on Spark)")

    row_count = io.write_spec(output, TARGET)
    output.unpersist()

    result.row_count = row_count
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "STG_TXN_SUMMARY loaded", rowcount=row_count)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", row_count, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
