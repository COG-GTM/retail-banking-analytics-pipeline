"""PySpark port of ``sas/01_sas_customer_segments.sas``.

Builds ``DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS``: one behavioural segment row per **active**
customer, produced by engineering six features from ``ETL_STAGING_DB.STG_CUSTOMER_360``,
standardising them, clustering them into five segments and labelling the clusters by their
average (standardised) balance.

Legacy construct mapping
------------------------
* ``PROC STDIZE method=std`` + ``PROC FASTCLUS maxclusters=5 maxiter=50 converge=0.001 least=2``
  -> :class:`~pyspark.ml.feature.VectorAssembler` + ``StandardScaler(withMean, withStd)`` +
  ``KMeans(k=5, maxIter=50, tol=0.001, seed=KMEANS_SEED)`` over the same six features, in the
  same order. ``StandardScaler`` uses the sample standard deviation, like ``PROC STDIZE``.
* ``today()`` -> ``run_date_col(config.run_date)``; ``datetime()`` -> the job's load timestamp.
* ``round(x, 0.01)`` -> :func:`common.functions.sas_round`.
* ``DELETE FROM ... ; PROC APPEND`` (full refresh) -> an ``overwrite`` write.

Determinism
-----------
* The k-means ``seed`` is pinned (:data:`KMEANS_SEED`) so a re-run of the job on the same input
  produces byte-identical output. FASTCLUS (``replace=full``, least-squares) and Spark's
  ``k-means||`` initialisation are different algorithms, so the **cluster numbering** is not
  comparable with SAS; only the *labelling*, which is re-derived from the ordered cluster
  profile exactly as the legacy code does, is.
* ``PROC SQL ... GROUP BY CLUSTER ORDER BY AVG_BALANCE DESC`` is ambiguous when two clusters
  share an average balance, so ``CLUSTER ASC`` is appended as a deterministic tiebreaker.

SAS missing-value semantics reproduced here
-------------------------------------------
* A SAS missing value sorts *below* every number, so ``if X < 12`` is **true** for a missing
  ``X``: NULL ``TENURE_MONTHS``/``AGE``/``TOTAL_BALANCE`` therefore fall into the first bucket
  (``NEW (<1yr)`` / ``GEN_Z`` / ``LOW``) and a NULL ``ACCT_RATIO`` satisfies ``< 0.50`` in the
  retention-risk rule.
* ``max(TOTAL_BALANCE, 1)`` ignores missing operands, so a NULL balance yields ``log(1) = 0``.
* ``NUM_ACTIVE_ACCOUNTS / max(NUM_ACCOUNTS, 1)`` propagates missing, so a NULL active-account
  count yields a NULL ``ACCT_RATIO``.
* ``PROC STDIZE`` computes its mean/std over non-missing values only and ``PROC FASTCLUS``
  assigns an incomplete observation using its non-missing coordinates. The port fills a missing
  feature with that feature's mean (i.e. the standardised value 0) before assembling, so the
  customer is still clustered and still reaches the output.

Deviation from the legacy source (deliberate, see MIGRATION notes in the PR)
----------------------------------------------------------------------------
STEP 6 of the SAS program reads ``c.LOG_BALANCE``, ``c.TENURE_MONTHS``, ``c.PRODUCT_BREADTH``
and ``c.ACCT_RATIO`` from ``WORK.CUST_CLUSTERED``, whose six clustering variables were
*overwritten with z-scores* by ``PROC STDIZE`` in STEP 3. Read literally, ``LIFETIME_VALUE_SCORE``
and ``ENGAGEMENT_SCORE`` would be products of z-scores and both propensity flags would test
z-scores against raw thresholds (``>= 0.75``, ``< 0.50``). This port uses the **raw** engineered
features for the scores and flags, which is what the published data product contains. The
cluster *profile* ordering does use the standardised ``LOG_BALANCE``, exactly as the SAS reads
it (the ordering is identical either way, standardisation being monotone).
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.functions import run_date_col, sas_round
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import enforce_schema
from common.validation import abort_on_failure, validate_table

JOB_NAME = "01_sas_customer_segments"
STEP_NAME = "FULL_LOAD"
TARGET = schemas.CUSTOMER_SEGMENTS
SOURCE = schemas.STG_CUSTOMER_360

#: ``where CUSTOMER_STATUS = 'A'`` - STEP 1 of the SAS program.
ACTIVE_CUSTOMER_STATUS = "A"
#: ``%let MODEL_VERSION = SEG_V3.2;``
MODEL_VERSION = "SEG_V3.2"
#: ``var LOG_BALANCE TENURE_MONTHS CREDIT_UTILIZATION_PCT PRODUCT_BREADTH ACCT_RATIO AGE`` -
#: the order is part of the contract with ``PROC STDIZE`` / ``PROC FASTCLUS``.
FEATURE_COLUMNS: tuple[str, ...] = (
    "LOG_BALANCE",
    "TENURE_MONTHS",
    "CREDIT_UTILIZATION_PCT",
    "PRODUCT_BREADTH",
    "ACCT_RATIO",
    "AGE",
)
#: ``maxclusters=5``
NUM_CLUSTERS = 5
#: ``maxiter=50``
MAX_ITERATIONS = 50
#: ``converge=0.001``
CONVERGENCE_TOLERANCE = 0.001
#: FASTCLUS is deterministic; Spark's ``k-means||`` initialisation is not, so the seed is pinned.
KMEANS_SEED = 20260410
#: ``if _N_ = 1 then SEGMENT_NAME = 'PREMIUM_WEALTH'; ... else SEGMENT_NAME = 'VALUE_BASIC';``
SEGMENT_LABELS: tuple[str, ...] = (
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
)
#: ``SUBSEGMENT_ID = 0;  /* Placeholder for future sub-segmentation */``
SUBSEGMENT_ID = 0
#: ``DIGITAL_ADOPTION_SCORE = 0;`` and ``'' as CHANNEL_PREFERENCE`` - placeholders in the legacy
#: program ("enriched later by txn analytics"); kept as placeholders.
DIGITAL_ADOPTION_SCORE = 0
CHANNEL_PREFERENCE = ""

#: ``if TENURE_MONTHS < 12 ... else if < 36 ... else if < 84 ... else``
TENURE_GROUPS: tuple[tuple[float, str], ...] = (
    (12, "NEW (<1yr)"),
    (36, "DEVELOPING (1-3yr)"),
    (84, "ESTABLISHED (3-7yr)"),
)
TENURE_GROUP_DEFAULT = "LOYAL (7yr+)"
#: ``if AGE < 25 ... else if < 41 ... else if < 57 ... else if < 76 ... else``
AGE_GROUPS: tuple[tuple[float, str], ...] = (
    (25, "GEN_Z"),
    (41, "MILLENNIAL"),
    (57, "GEN_X"),
    (76, "BOOMER"),
)
AGE_GROUP_DEFAULT = "SILENT"
#: ``if TOTAL_BALANCE < 1000 ... else if < 10000 ... else if < 100000 ... else``
BALANCE_TIERS: tuple[tuple[float, str], ...] = (
    (1000, "LOW"),
    (10000, "MODERATE"),
    (100000, "AFFLUENT"),
)
BALANCE_TIER_DEFAULT = "HIGH_NET_WORTH"

#: ``case when f.PRODUCT_BREADTH < 0.50 and c.ACCT_RATIO >= 0.75 then 'Y' else 'N' end``
CROSS_SELL_BREADTH_MAX = 0.50
CROSS_SELL_ACCT_RATIO_MIN = 0.75
#: ``case when f.BALANCE_TIER = 'MODERATE' and f.TENURE_GROUP not in ('NEW (<1yr)') ...``
UPSELL_BALANCE_TIER = "MODERATE"
UPSELL_EXCLUDED_TENURE_GROUPS = ("NEW (<1yr)",)
#: ``case when c.ACCT_RATIO < 0.50 and f.TENURE_MONTHS >= 60 then 'Y' else 'N' end``
RETENTION_ACCT_RATIO_MAX = 0.50
RETENTION_TENURE_MONTHS_MIN = 60
#: ``round(c.LOG_BALANCE * c.TENURE_MONTHS * c.PRODUCT_BREADTH * 10, 0.01)``
LTV_MULTIPLIER = 10

_STD_PREFIX = "STD_"
_NUMERIC_PREFIX = "_num_"
_MEAN_PREFIX = "_mean_"
_FEATURES_VECTOR = "_features"
_SCALED_VECTOR = "_scaled"
_CLUSTER = "CLUSTER"


def _sas_lt(column: Column, threshold: float) -> Column:
    """``if X < threshold`` with SAS semantics: a missing value sorts below every number."""

    return column.isNull() | (column < F.lit(threshold))


def _sas_bucket(column: Column, buckets: tuple[tuple[float, str], ...], default: str) -> Column:
    """The SAS ``if/else if/else`` bucket cascades of STEP 2, evaluated in source order."""

    expression = F.lit(default)
    for threshold, label in reversed(buckets):
        expression = F.when(_sas_lt(column, threshold), F.lit(label)).otherwise(expression)
    return expression


def transform_features(customer_360: DataFrame) -> DataFrame:
    """STEP 1 + STEP 2: extract the active customers and engineer the clustering features.

    ``PRODUCT_BREADTH`` is SAS ``mean()`` over four 0/1 booleans; the flags are ``NOT NULL``
    with default ``'N'`` in the DDL, so the mean is never missing and the divisor is always 4.
    """

    active = customer_360.filter(F.col("CUSTOMER_STATUS") == F.lit(ACTIVE_CUSTOMER_STATUS))

    product_breadth = sum(
        (F.col(flag).eqNullSafe(F.lit("Y"))).cast("int")
        for flag in ("HAS_CHECKING", "HAS_SAVINGS", "HAS_CREDIT", "HAS_LOAN")
    ) / F.lit(4.0)

    total_balance = F.col("TOTAL_BALANCE").cast("double")
    num_accounts = F.col("NUM_ACCOUNTS").cast("double")

    return active.select(
        F.col("CUSTOMER_ID"),
        F.col("AGE"),
        F.col("TENURE_MONTHS"),
        F.col("CUSTOMER_STATUS"),
        F.col("SEGMENT_CODE"),
        F.col("STATE_CODE"),
        F.col("NUM_ACCOUNTS"),
        F.col("NUM_ACTIVE_ACCOUNTS"),
        F.col("HAS_CHECKING"),
        F.col("HAS_SAVINGS"),
        F.col("HAS_CREDIT"),
        F.col("HAS_LOAN"),
        F.col("TOTAL_BALANCE"),
        F.col("TOTAL_CREDIT_LIMIT"),
        F.col("CREDIT_UTILIZATION_PCT"),
        product_breadth.alias("PRODUCT_BREADTH"),
        _sas_bucket(F.col("TENURE_MONTHS"), TENURE_GROUPS, TENURE_GROUP_DEFAULT).alias(
            "TENURE_GROUP"
        ),
        _sas_bucket(F.col("AGE"), AGE_GROUPS, AGE_GROUP_DEFAULT).alias("AGE_GROUP"),
        _sas_bucket(F.col("TOTAL_BALANCE"), BALANCE_TIERS, BALANCE_TIER_DEFAULT).alias(
            "BALANCE_TIER"
        ),
        F.lit(DIGITAL_ADOPTION_SCORE).alias("DIGITAL_ADOPTION_SCORE"),
        # log(max(TOTAL_BALANCE, 1)): SAS max() ignores missing operands, so NULL -> log(1) = 0.
        F.log(F.greatest(F.coalesce(total_balance, F.lit(1.0)), F.lit(1.0))).alias("LOG_BALANCE"),
        # NUM_ACTIVE_ACCOUNTS / max(NUM_ACCOUNTS, 1): a missing numerator propagates.
        (
            F.col("NUM_ACTIVE_ACCOUNTS").cast("double")
            / F.greatest(F.coalesce(num_accounts, F.lit(1.0)), F.lit(1.0))
        ).alias("ACCT_RATIO"),
    )


def transform_clusters(features: DataFrame, *, seed: int = KMEANS_SEED) -> DataFrame:
    """STEP 3 + STEP 4: standardise the six features and cluster them into five segments.

    Returns ``features`` plus the assigned ``CLUSTER`` and one ``STD_<FEATURE>`` column per
    feature, which is what ``WORK.CUST_CLUSTERED`` holds in the legacy program (``PROC STDIZE``
    overwrites its ``var`` variables in place, and ``PROC FASTCLUS`` copies the input through).

    A missing feature is filled with that feature's mean over the non-missing rows - i.e. with
    the standardised value 0 - so that, as in FASTCLUS, an incomplete observation is still
    assigned to a cluster instead of being dropped from the data product.
    """

    numeric = features.select(
        "*",
        *[F.col(name).cast("double").alias(f"{_NUMERIC_PREFIX}{name}") for name in FEATURE_COLUMNS],
    )
    means = numeric.agg(
        *[
            F.mean(f"{_NUMERIC_PREFIX}{name}").alias(f"{_MEAN_PREFIX}{name}")
            for name in FEATURE_COLUMNS
        ]
    )
    imputed = numeric.crossJoin(F.broadcast(means)).select(
        *features.columns,
        *[
            F.coalesce(
                F.col(f"{_NUMERIC_PREFIX}{name}"), F.col(f"{_MEAN_PREFIX}{name}"), F.lit(0.0)
            ).alias(f"{_NUMERIC_PREFIX}{name}")
            for name in FEATURE_COLUMNS
        ],
    )

    assembler = VectorAssembler(
        inputCols=[f"{_NUMERIC_PREFIX}{name}" for name in FEATURE_COLUMNS],
        outputCol=_FEATURES_VECTOR,
    )
    scaler = StandardScaler(
        inputCol=_FEATURES_VECTOR, outputCol=_SCALED_VECTOR, withMean=True, withStd=True
    )
    kmeans = KMeans(
        featuresCol=_SCALED_VECTOR,
        predictionCol=_CLUSTER,
        k=NUM_CLUSTERS,
        maxIter=MAX_ITERATIONS,
        tol=CONVERGENCE_TOLERANCE,
        seed=seed,
    )

    assembled = assembler.transform(imputed)
    scaled = scaler.fit(assembled).transform(assembled)
    clustered = kmeans.fit(scaled).transform(scaled)

    standardised = vector_to_array(F.col(_SCALED_VECTOR))
    return clustered.select(
        *features.columns,
        F.col(_CLUSTER),
        *[
            standardised.getItem(position).alias(f"{_STD_PREFIX}{name}")
            for position, name in enumerate(FEATURE_COLUMNS)
        ],
    )


def transform_cluster_profiles(clustered: DataFrame) -> DataFrame:
    """STEP 5, ``WORK.CLUSTER_PROFILES``: one row per cluster over the standardised features."""

    return clustered.groupBy(_CLUSTER).agg(
        F.count(F.lit(1)).alias("N"),
        F.avg(f"{_STD_PREFIX}LOG_BALANCE").alias("AVG_BALANCE"),
        F.avg(f"{_STD_PREFIX}TENURE_MONTHS").alias("AVG_TENURE"),
        F.avg(f"{_STD_PREFIX}PRODUCT_BREADTH").alias("AVG_BREADTH"),
        F.avg(f"{_STD_PREFIX}CREDIT_UTILIZATION_PCT").alias("AVG_CREDIT_UTIL"),
    )


def transform_cluster_labels(clustered: DataFrame) -> DataFrame:
    """STEP 5, ``WORK.SEGMENT_LABELS``: label the clusters by descending average balance.

    ``_N_ = 1..5`` over ``ORDER BY AVG_BALANCE DESC`` becomes ``row_number()`` over the same
    ordering, with ``CLUSTER ASC`` appended as a deterministic tiebreaker (the legacy ordering
    is ambiguous when two clusters share an average standardised ``LOG_BALANCE``). Ranks beyond
    the fifth fall into the SAS ``else`` branch, ``VALUE_BASIC``.
    """

    ordering = Window.orderBy(F.col("AVG_BALANCE").desc(), F.col(_CLUSTER).asc())
    ranked = transform_cluster_profiles(clustered).withColumn(
        "_rank", F.row_number().over(ordering)
    )

    segment_name = F.lit(SEGMENT_LABELS[-1])
    for position, label in reversed(list(enumerate(SEGMENT_LABELS[:-1], start=1))):
        segment_name = F.when(F.col("_rank") == F.lit(position), F.lit(label)).otherwise(
            segment_name
        )

    return ranked.select(
        F.col(_CLUSTER),
        segment_name.alias("SEGMENT_NAME"),
        F.lit(SUBSEGMENT_ID).alias("SUBSEGMENT_ID"),
    )


def transform_customer_segments(
    features: DataFrame,
    *,
    run_date: date,
    load_ts: Column | None = None,
    seed: int = KMEANS_SEED,
) -> DataFrame:
    """STEP 3 - STEP 6: cluster, label and score, projected onto the DDL contract.

    ``features`` is the output of :func:`transform_features`. The legacy program joins
    ``WORK.CUST_CLUSTERED`` back to ``WORK.CUST_FEATURES`` on ``CUSTOMER_ID``; here the feature
    columns are carried through the clustering step, which is equivalent and avoids a shuffle.
    """

    load_ts = F.current_timestamp() if load_ts is None else load_ts
    clustered = transform_clusters(features, seed=seed)
    labels = transform_cluster_labels(clustered)

    acct_ratio = F.col("ACCT_RATIO")
    product_breadth = F.col("PRODUCT_BREADTH")

    cross_sell = (product_breadth < F.lit(CROSS_SELL_BREADTH_MAX)) & (
        acct_ratio >= F.lit(CROSS_SELL_ACCT_RATIO_MIN)
    )
    upsell = (F.col("BALANCE_TIER") == F.lit(UPSELL_BALANCE_TIER)) & (
        ~F.col("TENURE_GROUP").isin(*UPSELL_EXCLUDED_TENURE_GROUPS)
    )
    retention_risk = _sas_lt(acct_ratio, RETENTION_ACCT_RATIO_MAX) & (
        F.col("TENURE_MONTHS") >= F.lit(RETENTION_TENURE_MONTHS_MIN)
    )

    scored = clustered.join(F.broadcast(labels), on=_CLUSTER, how="inner").select(
        F.col("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col(_CLUSTER).alias("SEGMENT_ID"),
        F.col("SUBSEGMENT_ID"),
        sas_round(
            F.col("LOG_BALANCE") * F.col("TENURE_MONTHS") * product_breadth * F.lit(LTV_MULTIPLIER)
        ).alias("LIFETIME_VALUE_SCORE"),
        sas_round(acct_ratio * F.lit(100)).alias("ENGAGEMENT_SCORE"),
        F.col("DIGITAL_ADOPTION_SCORE"),
        sas_round(product_breadth * F.lit(100)).alias("PRODUCT_BREADTH_INDEX"),
        F.col("TENURE_GROUP"),
        F.col("AGE_GROUP"),
        F.col("BALANCE_TIER"),
        F.lit(CHANNEL_PREFERENCE).alias("CHANNEL_PREFERENCE"),
        F.when(cross_sell, F.lit("Y")).otherwise(F.lit("N")).alias("CROSS_SELL_FLAG"),
        F.when(upsell, F.lit("Y")).otherwise(F.lit("N")).alias("UPSELL_FLAG"),
        F.when(retention_risk, F.lit("Y")).otherwise(F.lit("N")).alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        run_date_col(run_date).alias("EFFECTIVE_DATE"),
        load_ts.alias("LOAD_TS"),
    )
    return enforce_schema(scored, TARGET)


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the SAS program's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    audit.log_step(JOB_NAME, "START", "Beginning customer segmentation pipeline")

    audit.log_step(JOB_NAME, "START", "Extracting STG_CUSTOMER_360")
    customer_360 = io.read_spec(SOURCE)
    features = transform_features(customer_360).persist()
    extract_rows = features.count()
    audit.log_step(JOB_NAME, "SUCCESS", "Extracted staging data", rowcount=extract_rows)

    audit.log_step(JOB_NAME, "START", "Engineering features")
    audit.log_step(JOB_NAME, "START", f"Running FASTCLUS k={NUM_CLUSTERS}")
    audit.log_step(JOB_NAME, "START", "Labelling segments")
    output = transform_customer_segments(features, run_date=config.run_date).persist()

    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=TARGET.primary_index,
        not_null=("CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"),
        min_rows=config.min_rows,
    )
    result.validation = validation
    audit.log_step(JOB_NAME, "SUCCESS", "Segment table built", rowcount=validation.row_count)
    if not validation.passed:
        audit.log_step(JOB_NAME, "ERROR", "Validation failed - aborting load")
    abort_on_failure(validation)

    audit.log_step(JOB_NAME, "START", f"Loading {TARGET.qualified_name}")
    # DELETE FROM ... + PROC APPEND FORCE: a full refresh of the data product.
    row_count = io.write_spec(output, TARGET, mode="overwrite")
    output.unpersist()
    features.unpersist()

    result.row_count = row_count
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "Pipeline complete", rowcount=row_count)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", row_count, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
