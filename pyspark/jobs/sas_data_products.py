"""PySpark port of ``sas/04_sas_data_products.sas``.

Assembles ``DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE`` - the enterprise golden record - by
merging the ``ETL_STAGING_DB.STG_CUSTOMER_360`` base with the three certified data products
(``CUSTOMER_SEGMENTS``, ``TRANSACTION_ANALYTICS``, ``CUSTOMER_RISK_SCORES``).

The legacy ``data step`` ``MERGE ... BY CUSTOMER_ID`` with ``IN=`` flags is a four-way full
outer join carrying presence indicators, followed by ``if _base;``. It is ported literally:
:func:`transform_master_profile` full-outer-joins the four sources, keeps the rows present in
the base, and applies the ``IN=`` defaults **per missing source** (not per missing column), so a
source row that exists but carries a NULL column keeps that NULL.

Every source is customer-grain, so none of them is a broadcastable dimension; the joins stay
hash/sort-merge joins on ``CUSTOMER_ID`` and the whole transform is expressed in the DataFrame
API with no ``collect()``, UDF or Python loop over data.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.functions import run_date_col, sas_round
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import enforce_schema
from common.validation import abort_on_failure, validate_table

JOB_NAME = "04_sas_data_products"
STEP_NAME = "FULL_LOAD"
#: ``%log_step(step=04_MASTER_PROFILE, ...)`` - the audit step label used by the SAS script
LOG_STEP = "04_MASTER_PROFILE"
TARGET = schemas.CUSTOMER_MASTER_PROFILE

#: ``%let MODEL_VERSION = MASTER_V1.5;`` - sas/04_sas_data_products.sas:30
MODEL_VERSION = "MASTER_V1.5"
#: ``where CUSTOMER_STATUS = 'A'`` on the base projection - line 56
ACTIVE_CUSTOMER_STATUS = "A"
#: ``calculated FULL_NAME length=120`` - line 44
FULL_NAME_LENGTH = 120
#: ``if not _seg`` defaults - lines 125-130
DEFAULT_SEGMENT_NAME = "UNCLASSIFIED"
#: ``if not _txn`` defaults - lines 134-138
DEFAULT_TOP_SPEND_CATEGORY = ""
#: ``if not _risk`` defaults - lines 143-145 (the two SAS missings `.` are SQL NULL, not 0)
DEFAULT_RISK_TIER = "UNKNOWN"
DEFAULT_FLAG = "N"
#: ``%validate_table(... key_cols= not_null= min_rows=1000)`` - lines 194-197
KEY_COLUMNS = ("CUSTOMER_ID",)
NOT_NULL_COLUMNS = ("CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS")
#: ``COLLECT STATISTICS ... by teradata`` pass-through - lines 231-233
COLLECT_STATISTICS_TARGETS = (
    "DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE (CUSTOMER_ID)",
    "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS (SEGMENT_NAME)",
    "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES (RISK_TIER)",
)

_BASE = "_base"
_SEG = "_seg"
_TXN = "_txn"
_RISK = "_risk"


def _sas_trim(column: Column) -> Column:
    """``trim(x)`` on a SAS character variable: a missing value is the empty string."""

    return F.coalesce(F.trim(column), F.lit(""))


def transform_base(customer_360: DataFrame) -> DataFrame:
    """``WORK.BASE``: the active-customer projection of ``STG_CUSTOMER_360`` (lines 41-57).

    ``FULL_NAME`` is ``trim(FIRST_NAME) || ' ' || trim(LAST_NAME)`` declared ``length=120``, so
    it is truncated to 120 characters. SAS character missings are empty strings, so a customer
    with no first name yields ``" Lastname"`` rather than NULL.
    """

    full_name = F.substring(
        F.concat(_sas_trim(F.col("FIRST_NAME")), F.lit(" "), _sas_trim(F.col("LAST_NAME"))),
        1,
        FULL_NAME_LENGTH,
    )
    return customer_360.filter(F.col("CUSTOMER_STATUS") == ACTIVE_CUSTOMER_STATUS).select(
        F.col("CUSTOMER_ID"),
        full_name.alias("FULL_NAME"),
        F.col("AGE"),
        F.col("STATE_CODE"),
        F.col("CUSTOMER_SINCE"),
        F.col("TENURE_MONTHS"),
        F.col("CUSTOMER_STATUS"),
        F.col("NUM_ACCOUNTS").alias("TOTAL_ACCOUNTS"),
        F.col("NUM_ACTIVE_ACCOUNTS").alias("ACTIVE_ACCOUNTS"),
        F.col("TOTAL_BALANCE"),
        F.col("TOTAL_CREDIT_LIMIT"),
        F.col("CREDIT_UTILIZATION_PCT"),
        F.lit(True).alias(_BASE),
    )


def transform_segments(segments: DataFrame) -> DataFrame:
    """``WORK.SEGMENTS``: the segment columns consumed by the golden record (lines 60-70)."""

    return segments.select(
        F.col("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col("LIFETIME_VALUE_SCORE"),
        F.col("ENGAGEMENT_SCORE"),
        F.col("CROSS_SELL_FLAG"),
        F.col("UPSELL_FLAG"),
        F.col("RETENTION_RISK_FLAG"),
        F.lit(True).alias(_SEG),
    )


def transform_txn_analytics(transaction_analytics: DataFrame, run_date: date) -> DataFrame:
    """``WORK.TXN``: transaction analytics for the current period only (lines 73-83).

    ``where EFFECTIVE_DATE = today()`` becomes ``EFFECTIVE_DATE = run_date``. This is a legacy
    quirk that is preserved: the job assumes the upstream product was built on the same day, so
    a re-run on any later date silently drops every transaction row and the profile is loaded
    with the ``if not _txn`` zero defaults instead.
    """

    return (
        transaction_analytics.filter(F.col("EFFECTIVE_DATE") == run_date_col(run_date))
        .select(
            F.col("CUSTOMER_ID"),
            F.col("TOTAL_TRANSACTIONS").alias("MONTHLY_TRANSACTIONS"),
            F.col("TOTAL_DEBIT_AMT").alias("MONTHLY_SPEND"),
            F.col("NET_CASH_FLOW"),
            F.col("TOP_SPEND_CATEGORY"),
            F.col("DIGITAL_TXN_PCT"),
        )
        .withColumn(_TXN, F.lit(True))
    )


def transform_risk_scores(risk_scores: DataFrame) -> DataFrame:
    """``WORK.RISK``: the risk columns consumed by the golden record (lines 86-94)."""

    return risk_scores.select(
        F.col("CUSTOMER_ID"),
        F.col("COMPOSITE_RISK_SCORE"),
        F.col("RISK_TIER"),
        F.col("PROBABILITY_OF_DEFAULT"),
        F.col("WATCH_LIST_FLAG"),
        F.lit(True).alias(_RISK),
    )


def merge_by_customer_id(
    base: DataFrame, segments: DataFrame, txn: DataFrame, risk: DataFrame
) -> DataFrame:
    """``merge BASE(in=_base) SEGMENTS(in=_seg) TXN(in=_txn) RISK(in=_risk); by CUSTOMER_ID;``.

    Successive full outer joins keep the ``IN=`` semantics explicit and testable; ``if _base;``
    (line 121) is the final filter. The presence flags are materialised as booleans so the
    defaulting below can key off "the source had no row" rather than "the column is NULL".

    The legacy ``MERGE`` assumes one row per ``CUSTOMER_ID`` in each source (all four are keyed
    on it); duplicates would produce a SAS many-to-many merge, which is not reproduced here.
    """

    merged = (
        base.join(segments, on="CUSTOMER_ID", how="full_outer")
        .join(txn, on="CUSTOMER_ID", how="full_outer")
        .join(risk, on="CUSTOMER_ID", how="full_outer")
    )
    present = {flag: F.coalesce(F.col(flag), F.lit(False)) for flag in (_BASE, _SEG, _TXN, _RISK)}
    return merged.withColumns(present).filter(F.col(_BASE))


def transform_master_profile(
    customer_360: DataFrame,
    segments: DataFrame,
    transaction_analytics: DataFrame,
    risk_scores: DataFrame,
    *,
    run_date: date,
    load_ts: Column | None = None,
) -> DataFrame:
    """Full port of the ``data WORK.MASTER_PROFILE`` step (lines 111-154)."""

    load_ts = F.current_timestamp() if load_ts is None else load_ts
    merged = merge_by_customer_id(
        transform_base(customer_360),
        transform_segments(segments),
        transform_txn_analytics(transaction_analytics, run_date),
        transform_risk_scores(risk_scores),
    )

    has_seg, has_txn, has_risk = F.col(_SEG), F.col(_TXN), F.col(_RISK)

    def defaulted(present: Column, column: str, default: Column) -> Column:
        return F.when(present, F.col(column)).otherwise(default).alias(column)

    projected = merged.select(
        F.col("CUSTOMER_ID"),
        F.col("FULL_NAME"),
        F.col("AGE"),
        F.col("STATE_CODE"),
        F.col("CUSTOMER_SINCE"),
        F.col("TENURE_MONTHS"),
        F.col("CUSTOMER_STATUS"),
        # if not _seg
        defaulted(has_seg, "SEGMENT_NAME", F.lit(DEFAULT_SEGMENT_NAME)),
        defaulted(has_seg, "LIFETIME_VALUE_SCORE", F.lit(0)),
        defaulted(has_seg, "ENGAGEMENT_SCORE", F.lit(0)),
        F.col("TOTAL_ACCOUNTS"),
        F.col("ACTIVE_ACCOUNTS"),
        F.col("TOTAL_BALANCE"),
        F.col("TOTAL_CREDIT_LIMIT"),
        F.col("CREDIT_UTILIZATION_PCT"),
        # if not _txn
        defaulted(has_txn, "MONTHLY_TRANSACTIONS", F.lit(0)),
        defaulted(has_txn, "MONTHLY_SPEND", F.lit(0)),
        defaulted(has_txn, "NET_CASH_FLOW", F.lit(0)),
        defaulted(has_txn, "TOP_SPEND_CATEGORY", F.lit(DEFAULT_TOP_SPEND_CATEGORY)),
        defaulted(has_txn, "DIGITAL_TXN_PCT", F.lit(0)),
        # if not _risk - the two SAS missings map to NULL, not to 0
        defaulted(has_risk, "COMPOSITE_RISK_SCORE", F.lit(None)),
        defaulted(has_risk, "RISK_TIER", F.lit(DEFAULT_RISK_TIER)),
        defaulted(has_risk, "PROBABILITY_OF_DEFAULT", F.lit(None)),
        defaulted(has_risk, "WATCH_LIST_FLAG", F.lit(DEFAULT_FLAG)),
        defaulted(has_seg, "CROSS_SELL_FLAG", F.lit(DEFAULT_FLAG)),
        defaulted(has_seg, "UPSELL_FLAG", F.lit(DEFAULT_FLAG)),
        defaulted(has_seg, "RETENTION_RISK_FLAG", F.lit(DEFAULT_FLAG)),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        run_date_col(run_date).alias("EFFECTIVE_DATE"),
        load_ts.alias("LOAD_TS"),
    )
    return enforce_schema(projected, TARGET)


# ------------------------------------------------------------------------------------------
# STEP 3: the three PROC SQL data-quality reports (lines 163-188)
# ------------------------------------------------------------------------------------------


def transform_segment_distribution(master_profile: DataFrame) -> DataFrame:
    """``select SEGMENT_NAME, count(*) as N, round(mean(LIFETIME_VALUE_SCORE), 0.01) as AVG_LTV``.

    ``order by N desc`` ties are broken by ``SEGMENT_NAME`` ascending so the report is
    deterministic (the legacy ordering is ambiguous when two segments have the same count).
    """

    return (
        master_profile.groupBy("SEGMENT_NAME")
        .agg(
            F.count(F.lit(1)).alias("N"),
            sas_round(F.avg("LIFETIME_VALUE_SCORE")).alias("AVG_LTV"),
        )
        .orderBy(F.col("N").desc(), F.col("SEGMENT_NAME").asc())
    )


def transform_risk_tier_distribution(master_profile: DataFrame) -> DataFrame:
    """``select RISK_TIER, count(*) as N, round(mean(COMPOSITE_RISK_SCORE), 0.01) as AVG_SCORE``.

    ``order by AVG_SCORE desc`` ties (including the all-NULL ``UNKNOWN`` tier) are broken by
    ``RISK_TIER`` ascending for determinism.
    """

    return (
        master_profile.groupBy("RISK_TIER")
        .agg(
            F.count(F.lit(1)).alias("N"),
            sas_round(F.avg("COMPOSITE_RISK_SCORE")).alias("AVG_SCORE"),
        )
        .orderBy(F.col("AVG_SCORE").desc(), F.col("RISK_TIER").asc())
    )


def transform_completeness_check(master_profile: DataFrame) -> DataFrame:
    """The completeness ``PROC SQL`` (lines 176-186), one row of counters.

    ``SEGMENT_NAME ne 'UNCLASSIFIED'`` and ``RISK_TIER ne 'UNKNOWN'`` use a NULL-safe inequality:
    a SAS character missing is the empty string, which *is* ``ne`` the default label and is
    therefore counted as populated.
    """

    def flagged(column: str) -> Column:
        return F.sum(F.when(F.col(column) == "Y", 1).otherwise(0))

    return master_profile.agg(
        F.count(F.lit(1)).alias("TOTAL"),
        F.sum(
            F.when(~F.col("SEGMENT_NAME").eqNullSafe(F.lit(DEFAULT_SEGMENT_NAME)), 1).otherwise(0)
        ).alias("HAS_SEGMENT"),
        F.sum(F.when(F.col("MONTHLY_TRANSACTIONS") > 0, 1).otherwise(0)).alias("HAS_TXN"),
        F.sum(
            F.when(~F.col("RISK_TIER").eqNullSafe(F.lit(DEFAULT_RISK_TIER)), 1).otherwise(0)
        ).alias("HAS_RISK_SCORE"),
        flagged("CROSS_SELL_FLAG").alias("CROSS_SELL_ELIGIBLE"),
        flagged("UPSELL_FLAG").alias("UPSELL_ELIGIBLE"),
        flagged("RETENTION_RISK_FLAG").alias("RETENTION_AT_RISK"),
        flagged("WATCH_LIST_FLAG").alias("ON_WATCH_LIST"),
    )


def data_quality_report(master_profile: DataFrame) -> dict[str, DataFrame]:
    """The three STEP 3 reports, keyed by their legacy ``title``."""

    return {
        "Master Profile - Segment Distribution": transform_segment_distribution(master_profile),
        "Master Profile - Risk Tier Distribution": transform_risk_tier_distribution(master_profile),
        "Master Profile - Completeness Check": transform_completeness_check(master_profile),
    }


def log_quality_report(audit: AuditLog, report: dict[str, DataFrame]) -> dict[str, list[dict]]:
    """Emit each report row through ``%log_step`` (the legacy reports printed to the SAS log).

    The reports are grouped aggregates (one row per segment / per risk tier / one row overall),
    so collecting them to the driver is bounded by the cardinality of those labels.
    """

    collected: dict[str, list[dict]] = {}
    for title, frame in report.items():
        rows = [row.asDict() for row in frame.collect()]
        collected[title] = rows
        for row in rows:
            audit.log_step(LOG_STEP, "SUCCESS", f"{title}: {row}")
    return collected


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the SAS script's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    audit.log_step(JOB_NAME, "START", "Building golden record")

    # STEP 1
    audit.log_step(JOB_NAME, "START", "Extracting upstream data products")
    customer_360 = io.read_spec(schemas.STG_CUSTOMER_360)
    segments = io.read_spec(schemas.CUSTOMER_SEGMENTS)
    transaction_analytics = io.read_spec(schemas.TRANSACTION_ANALYTICS)
    risk_scores = io.read_spec(schemas.CUSTOMER_RISK_SCORES)
    audit.log_step(JOB_NAME, "SUCCESS", "All upstream data extracted")

    # STEP 2
    audit.log_step(JOB_NAME, "START", "Merging data products")
    output = transform_master_profile(
        customer_360,
        segments,
        transaction_analytics,
        risk_scores,
        run_date=config.run_date,
    ).persist()
    row_count = output.count()
    audit.log_step(JOB_NAME, "SUCCESS", "Master profile built", rowcount=row_count)

    # STEP 3
    log_quality_report(audit, data_quality_report(output))

    # STEP 4
    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=KEY_COLUMNS,
        not_null=NOT_NULL_COLUMNS,
        min_rows=config.min_rows,
    )
    result.validation = validation
    if not validation.passed:
        audit.log_step(JOB_NAME, "ERROR", "Validation failed")
        output.unpersist()
        abort_on_failure(validation)

    # STEP 5: DELETE FROM ... + PROC APPEND = a full-refresh overwrite.
    audit.log_step(JOB_NAME, "START", f"Loading {TARGET.qualified_name}")
    written = io.write_spec(output, TARGET, mode="overwrite")
    output.unpersist()
    audit.log_step(JOB_NAME, "SUCCESS", "Golden record loaded", rowcount=written)

    # STEP 6: Spark maintains its own statistics.
    for target in COLLECT_STATISTICS_TARGETS:
        audit.log_step(JOB_NAME, "START", f"COLLECT STATISTICS (no-op on Spark): {target}")

    # STEP 7
    result.row_count = written
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "Full pipeline complete", rowcount=written)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", written, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
