"""PySpark port of ``sas/02_sas_txn_analytics.sas``.

Builds ``DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS``: one row per customer present in
``ETL_STAGING_DB.STG_TXN_SUMMARY`` (the SAS job does not filter on customer status, so
customers dropped by the customer-360 job are still analysed here), with the account-level
staging rows aggregated to customer level, a spend trend placeholder, a ``PROC RANK`` spend
percentile and a ``PROC MEANS`` IQR anomaly flag.

Legacy constructs and how they are mapped:

``PROC RANK groups=100``
    SAS computes ``FLOOR(rank * k / (n + 1))`` where ``rank`` is the observation's order rank
    -- the *average* order rank for tied values under the default ``TIES=MEAN`` -- and ``n``
    is the number of non-missing values. That formula is ported literally in
    :func:`transform_spend_percentile`, which makes tied ``TOTAL_DEBIT_AMT`` values share one
    group exactly as SAS does. ``ntile(100) - 1`` is the usual shorthand for this construct but
    it splits ties across buckets and it is not the same function: on the committed 500-customer
    extract the two disagree by 1 on 5 of the 500 ranks (order ranks 35, 70, 140, 275 and 280).
    Missing values get a missing group in SAS and are excluded from ``n``; both are reproduced.

``PROC MEANS ... median= qrange=``
    ``percentile_approx`` at 0.25/0.5/0.75 with an accuracy of 1,000,000, computed once over the
    whole population and cross-joined back onto every row: the port of the
    ``if _N_ = 1 then set WORK._TXN_STATS`` idiom. ``percentile_approx`` returns an observed
    value rather than interpolating between the two central observations the way
    ``PROC MEANS`` does for an even number of observations; the resulting median/IQR can
    therefore differ by less than one observation gap, which is immaterial to the
    ``median + 3 * IQR`` outlier cut-off.

``%sysfunc(intnx(month, today(), 0, beginning))`` formatted ``yymmn7.``
    :func:`reporting_period` -- ``date_format(run_date, 'yyyy-MM')``. Note that ``yymmn7.``
    literally renders ``202604``; the DDL contract (``VARCHAR(7)``, commented ``YYYY-MM``) and
    the reference output both carry ``2026-04``, so the hyphenated form is what is produced.
    The period drives both the partition value and the delete-by-period load pattern
    (``DELETE FROM ... WHERE REPORTING_PERIOD = ...`` + ``PROC APPEND``), which is a partition
    overwrite here.

Legacy quirks preserved verbatim (see ``pyspark/docs/LEGACY_INVENTORY.md`` section 5):

* ``ACTIVE_ACCOUNTS`` counts *accounts* whose last transaction is within 30 days, not
  transactions, and SAS missing-value ordering makes a missing ``DAYS_SINCE_LAST_TXN``
  satisfy ``<= 30``.
* ``TOP_SPEND_CATEGORY`` is ``MAX(TOP_MERCHANT_CATEGORY)``, i.e. the lexicographic maximum
  across the customer's accounts, not the category with the largest spend.
* ``DIGITAL_TXN_PCT`` is a transaction-weighted mean of ``PCT_WEB + PCT_MOBILE``.
* ``MONTHLY_SPEND_TREND`` is the net-cash-flow placeholder the legacy comment describes as a
  stand-in for a 3-month moving-window comparison; the placeholder is ported, not the intent.
* ``ANOMALY_FLAG`` is initialised to ``'N'`` before the IQR test, and ``SPEND_PERCENTILE`` is a
  0-99 group number rather than a true percentile.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.functions import run_date_col
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import enforce_schema
from common.validation import abort_on_failure, validate_table

JOB_NAME = "02_sas_txn_analytics"
STEP_NAME = "FULL_LOAD"
TARGET = schemas.TRANSACTION_ANALYTICS
SOURCE = schemas.STG_TXN_SUMMARY

#: ``%let MODEL_VERSION = TXN_V2.1;``
MODEL_VERSION = "TXN_V2.1"
#: ``sum(case when DAYS_SINCE_LAST_TXN <= 30 then 1 else 0 end) as ACTIVE_ACCOUNTS``
ACTIVE_DAYS_THRESHOLD = 30
#: ``if NET_CASH_FLOW > AVG_TRANSACTION_SIZE * 5 then MONTHLY_SPEND_TREND = 'UP';``
SPEND_TREND_MULTIPLIER = 5
#: ``INTEREST_INCOME = TOTAL_DEBIT_AMT * 0.02;  /* Simplified interest proxy */``
INTEREST_RATE_PROXY = 0.02
#: ``if TOTAL_DEBIT_AMT > _MEDIAN + (3 * _IQR) and _IQR > 0 then ANOMALY_FLAG = 'Y';``
ANOMALY_IQR_MULTIPLIER = 3
#: ``proc rank ... groups=100;`` - group values run from 0 to 99.
RANK_GROUPS = 100
#: ``percentile_approx`` accuracy for the ``PROC MEANS median= qrange=`` statistics.
PERCENTILE_ACCURACY = 1_000_000

#: ``%validate_table(... key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID REPORTING_PERIOD
#: TOTAL_TRANSACTIONS, min_rows=1000)`` - ``min_rows`` comes from the config.
VALIDATION_KEY_COLS = ("CUSTOMER_ID",)
VALIDATION_NOT_NULL_COLS = ("CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS")

TREND_UP = "UP"
TREND_DOWN = "DOWN"
TREND_STABLE = "STABLE"
ANOMALY_YES = "Y"
ANOMALY_NO = "N"

#: Every numeric in SAS is an 8-byte IEEE float, so the derived arithmetic (the divisions, the
#: interest proxy and the revenue sum) is carried out in double precision and only the DDL
#: contract rounds it to ``DECIMAL(p, s)``. Spark's exact decimal arithmetic would round
#: half-cent results the other way -- an average transaction size of exactly 131.645 is 131.65
#: as a decimal but 131.64 in SAS, whose nearest double is 131.64499999999998. The ``SUM``s
#: themselves stay in decimal: they are exact, and unlike a parallel double summation their
#: result does not depend on the order in which the partitions are merged, so the job stays
#: reproducible.
SAS_NUMERIC = "double"

_MEDIAN = "_MEDIAN"
_IQR = "_IQR"
_POPULATION = "_N_NONMISSING"
_STATS_COLUMNS = (_MEDIAN, _IQR, _POPULATION)


def reporting_period(run_date: date) -> str:
    """``%sysfunc(putn(%sysfunc(intnx(month, today(), 0, beginning)), yymmn7.))``.

    Derived from the pinned run date, never from ``current_date()``, so that the partition
    that is deleted and re-appended is the same one the rows are stamped with.
    """

    return run_date.strftime("%Y-%m")


def _sas_numeric(column: Column) -> Column:
    """Continue the calculation the way SAS does it: in 8-byte floating point."""

    return column.cast(SAS_NUMERIC)


def transform_customer_aggregates(txn_summary: DataFrame) -> DataFrame:
    """``STEP 2``: aggregate the account-level staging rows to customer level.

    Every aggregate is the legacy expression verbatim, including the three quirks:

    * ``ACTIVE_ACCOUNTS`` sums a 1/0 flag per *account* row. SAS orders a missing numeric
      below every non-missing value, so ``DAYS_SINCE_LAST_TXN <= 30`` is true when the column
      is missing; the SQL ``NULL <= 30`` would be unknown and fall to the ``else 0`` branch,
      so the missing case is spelled out to keep SAS semantics.
    * ``AVG_TRANSACTION_SIZE`` sums ``AMT_TOTAL_DEBIT + AMT_TOTAL_CREDIT`` per row before
      summing, so a row where either side is missing contributes nothing at all (the ``+``
      propagates the missing and ``SUM`` then skips it) - Spark behaves identically.
    * ``DIGITAL_TXN_PCT`` is a transaction-weighted average, written with the legacy order of
      operations (``* (PCT_WEB + PCT_MOBILE) / 100`` per row, then ``/ SUM(TXN_COUNT_TOTAL)``
      and ``* 100``).
    """

    txn_total = F.sum(F.col("TXN_COUNT_TOTAL"))
    debit_total = F.sum(F.col("AMT_TOTAL_DEBIT"))
    credit_total = F.sum(F.col("AMT_TOTAL_CREDIT"))
    combined_total = F.sum(F.col("AMT_TOTAL_DEBIT") + F.col("AMT_TOTAL_CREDIT"))
    digital_weighted = F.sum(
        F.col("TXN_COUNT_TOTAL") * (F.col("PCT_WEB") + F.col("PCT_MOBILE")) / F.lit(100)
    )
    is_active_account = F.col("DAYS_SINCE_LAST_TXN").isNull() | (
        F.col("DAYS_SINCE_LAST_TXN") <= F.lit(ACTIVE_DAYS_THRESHOLD)
    )

    return txn_summary.groupBy("CUSTOMER_ID").agg(
        F.count_distinct(F.col("ACCOUNT_ID")).alias("TOTAL_ACCOUNTS"),
        F.sum(F.when(is_active_account, F.lit(1)).otherwise(F.lit(0))).alias("ACTIVE_ACCOUNTS"),
        txn_total.alias("TOTAL_TRANSACTIONS"),
        debit_total.alias("TOTAL_DEBIT_AMT"),
        credit_total.alias("TOTAL_CREDIT_AMT"),
        (credit_total - debit_total).alias("NET_CASH_FLOW"),
        F.when(txn_total > 0, _sas_numeric(combined_total) / _sas_numeric(txn_total))
        .otherwise(F.lit(0))
        .alias("AVG_TRANSACTION_SIZE"),
        F.sum(F.col("AMT_TOTAL_FEES")).alias("TOTAL_FEES"),
        F.max(F.col("TOP_MERCHANT_CATEGORY")).alias("TOP_SPEND_CATEGORY"),
        F.when(txn_total > 0, _sas_numeric(digital_weighted) / txn_total * F.lit(100))
        .otherwise(F.lit(0))
        .alias("DIGITAL_TXN_PCT"),
    )


def transform_spend_trend(customer_txn: DataFrame) -> DataFrame:
    """``STEP 3``: spend trend, revenue components and the initialised anomaly flag.

    The legacy comment states that a full implementation would compare the current period
    against the previous one over a 3-month moving window; the shipped code simulates the
    trend from the net cash flow direction and that placeholder is what is ported. Both
    comparisons are strict, so a net cash flow exactly equal to +/- five average transactions
    is ``STABLE``.
    """

    threshold = F.col("AVG_TRANSACTION_SIZE") * F.lit(SPEND_TREND_MULTIPLIER)
    fee_income = F.col("TOTAL_FEES")
    interest_income = _sas_numeric(F.col("TOTAL_DEBIT_AMT")) * F.lit(INTEREST_RATE_PROXY)

    return customer_txn.withColumns(
        {
            "MONTHLY_SPEND_TREND": F.when(F.col("NET_CASH_FLOW") > threshold, F.lit(TREND_UP))
            .when(F.col("NET_CASH_FLOW") < -threshold, F.lit(TREND_DOWN))
            .otherwise(F.lit(TREND_STABLE)),
            "FEE_INCOME": fee_income,
            "INTEREST_INCOME": interest_income,
            "REVENUE_CONTRIBUTION": _sas_numeric(fee_income) + interest_income,
            "ANOMALY_FLAG": F.lit(ANOMALY_NO),
        }
    )


def transform_spend_percentile(
    customer_txn: DataFrame,
    *,
    value_col: str = "TOTAL_DEBIT_AMT",
    output_col: str = "SPEND_PERCENTILE",
    groups: int = RANK_GROUPS,
) -> DataFrame:
    """``STEP 4``: ``proc rank groups=100; var TOTAL_DEBIT_AMT; ranks SPEND_PERCENTILE;``.

    SAS assigns ``FLOOR(rank * k / (n + 1))`` with ``rank`` the average order rank of tied
    values (``TIES=MEAN``, the default) and ``n`` the number of non-missing values, which is
    what is implemented here: ``rank()`` gives the lowest order rank of a tie group and the
    tie width turns it into the average rank, so tied values always land in the same group.
    A missing ``TOTAL_DEBIT_AMT`` gets a missing group and is excluded from ``n``.

    The ranking is global by construction -- ``PROC RANK`` sorts the whole population -- so
    the ordered window is deliberately unpartitioned; it runs over one already-aggregated row
    per customer, and the tie width is computed with a partitioned (shuffled) window.
    """

    ordered = Window.orderBy(F.col(value_col).asc_nulls_last())
    ties = Window.partitionBy(value_col)
    population = Window.partitionBy()

    lowest_rank = F.rank().over(ordered)
    tie_width = F.count(F.col(value_col)).over(ties)
    mean_rank = lowest_rank + (tie_width - F.lit(1)) / F.lit(2)
    non_missing = F.count(F.col(value_col)).over(population)
    group = F.floor(mean_rank * F.lit(groups) / (non_missing + F.lit(1)))

    return customer_txn.withColumn(output_col, F.when(F.col(value_col).isNotNull(), group))


def transform_population_stats(customer_txn: DataFrame) -> DataFrame:
    """``STEP 5``: ``proc means noprint; var TOTAL_DEBIT_AMT; output median= qrange=;``.

    One row, computed once over the whole population, to be broadcast back onto every row.
    """

    quartiles = F.percentile_approx(
        _sas_numeric(F.col("TOTAL_DEBIT_AMT")),
        [0.25, 0.5, 0.75],
        F.lit(PERCENTILE_ACCURACY),
    ).alias("_QUARTILES")

    return customer_txn.agg(quartiles, F.count(F.col("TOTAL_DEBIT_AMT")).alias(_POPULATION)).select(
        F.col("_QUARTILES")[1].alias(_MEDIAN),
        (F.col("_QUARTILES")[2] - F.col("_QUARTILES")[0]).alias(_IQR),
        F.col(_POPULATION),
    )


def transform_anomaly_flag(customer_txn: DataFrame, stats: DataFrame) -> DataFrame:
    """``STEP 5``: ``if _N_ = 1 then set WORK._TXN_STATS`` followed by the IQR outlier test.

    ``if TOTAL_DEBIT_AMT > _MEDIAN + (3 * _IQR) and _IQR > 0 then ANOMALY_FLAG = 'Y';`` -
    otherwise the ``'N'`` initialised in ``STEP 3`` stands, which is also what a customer with
    a missing ``TOTAL_DEBIT_AMT`` keeps.
    """

    cutoff = F.col(_MEDIAN) + F.lit(ANOMALY_IQR_MULTIPLIER) * F.col(_IQR)
    is_anomaly = (F.col("TOTAL_DEBIT_AMT") > cutoff) & (F.col(_IQR) > 0)

    return (
        customer_txn.crossJoin(F.broadcast(stats))
        .withColumn(
            "ANOMALY_FLAG",
            F.when(is_anomaly, F.lit(ANOMALY_YES)).otherwise(F.col("ANOMALY_FLAG")),
        )
        .drop(*_STATS_COLUMNS)
    )


def transform_transaction_analytics(
    txn_summary: DataFrame,
    *,
    run_date: date,
    load_ts: Column | None = None,
) -> DataFrame:
    """Compose STEP 2 to STEP 5 and stamp the run metadata onto the data product."""

    load_ts = F.current_timestamp() if load_ts is None else load_ts
    customer_txn = transform_spend_trend(transform_customer_aggregates(txn_summary))
    ranked = transform_spend_percentile(customer_txn)
    flagged = transform_anomaly_flag(ranked, transform_population_stats(customer_txn))

    stamped = flagged.withColumns(
        {
            "REPORTING_PERIOD": F.lit(reporting_period(run_date)),
            "MODEL_VERSION": F.lit(MODEL_VERSION),
            "EFFECTIVE_DATE": run_date_col(run_date),
            "LOAD_TS": load_ts,
        }
    )
    return enforce_schema(stamped, TARGET)


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the SAS script's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    period = reporting_period(config.run_date)
    audit.log_step(JOB_NAME, "START", f"Period: {period}")

    txn_summary = io.read_spec(SOURCE).persist()
    audit.log_step(JOB_NAME, "SUCCESS", "Extracted STG_TXN_SUMMARY", rowcount=txn_summary.count())

    output = transform_transaction_analytics(txn_summary, run_date=config.run_date).persist()
    txn_summary.unpersist()
    audit.log_step(JOB_NAME, "SUCCESS", "Analytics table built", rowcount=output.count())

    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=VALIDATION_KEY_COLS,
        not_null=VALIDATION_NOT_NULL_COLS,
        min_rows=config.min_rows,
    )
    result.validation = validation
    if not validation.passed:
        audit.log_step(JOB_NAME, "ERROR", "Validation failed")
    abort_on_failure(validation)

    audit.log_step(JOB_NAME, "START", f"Loading {TARGET.qualified_name}")
    row_count = io.write_spec(
        output, TARGET, mode="overwrite", partition_values={"REPORTING_PERIOD": period}
    )
    output.unpersist()

    result.row_count = row_count
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "Pipeline complete", rowcount=row_count)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", row_count, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
