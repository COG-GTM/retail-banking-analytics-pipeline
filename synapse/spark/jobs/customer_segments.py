"""dp.CUSTOMER_SEGMENTS: Synapse Spark port of sas/01_sas_customer_segments.sas.

stg.STG_CUSTOMER_360 (active customers) -> feature engineering -> StandardScaler
(PROC STDIZE METHOD=STD) -> KMeans k=5 (PROC FASTCLUS) -> cluster profiling and
SEGMENT_LABELS -> scores and action flags -> dp.CUSTOMER_SEGMENTS.
"""

from __future__ import annotations

import datetime
import sys

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

JOB_NAME = "01_sas_customer_segments"
STEP_NAME = "01_CUSTOMER_SEG"
MODEL_VERSION = "SEG_V3.2"

SOURCE_SCHEMA, SOURCE_TABLE = "stg", "STG_CUSTOMER_360"
TARGET_SCHEMA, TARGET_TABLE = "dp", "CUSTOMER_SEGMENTS"

# PROC STDIZE / PROC FASTCLUS VAR statement, in SAS order.
CLUSTER_FEATURES = [
    "LOG_BALANCE",
    "TENURE_MONTHS",
    "CREDIT_UTILIZATION_PCT",
    "PRODUCT_BREADTH",
    "ACCT_RATIO",
    "AGE",
]
N_CLUSTERS = 5  # MAXCLUSTERS=5
MAX_ITER = 50  # MAXITER=50
CONVERGE = 0.001  # CONVERGE=0.001
SEED = 42
# Restarts with seeds SEED..SEED+N_INIT-1; the run with the lowest within-cluster SSE wins.
N_INIT = 60

# SEGMENT_LABELS: clusters ranked by mean LOG_BALANCE descending.
SEGMENT_LABELS = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]

# %validate_table(key_cols=CUSTOMER_ID, not_null=CUSTOMER_ID SEGMENT_NAME SEGMENT_ID, min_rows=1000)
KEY_COLS = ["CUSTOMER_ID"]
NOT_NULL_COLS = ["CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"]
SAS_MIN_ROWS = 1000

OUTPUT_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType(), False),
        StructField("SEGMENT_NAME", StringType()),
        StructField("SEGMENT_ID", ShortType()),
        StructField("SUBSEGMENT_ID", ShortType()),
        StructField("LIFETIME_VALUE_SCORE", DecimalType(10, 2)),
        StructField("ENGAGEMENT_SCORE", DecimalType(5, 2)),
        StructField("DIGITAL_ADOPTION_SCORE", DecimalType(5, 2)),
        StructField("PRODUCT_BREADTH_INDEX", DecimalType(5, 2)),
        StructField("TENURE_GROUP", StringType()),
        StructField("AGE_GROUP", StringType()),
        StructField("BALANCE_TIER", StringType()),
        StructField("CHANNEL_PREFERENCE", StringType()),
        StructField("CROSS_SELL_FLAG", StringType()),
        StructField("UPSELL_FLAG", StringType()),
        StructField("RETENTION_RISK_FLAG", StringType()),
        StructField("MODEL_VERSION", StringType()),
        StructField("EFFECTIVE_DATE", DateType()),
        StructField("LOAD_TS", TimestampType()),
    ]
)


def _lt_or_missing(col, bound) -> F.Column:
    """SAS `x < bound`: a missing numeric sorts below every number, so it is true."""
    return col.isNull() | (col < bound)


def _engineer_features(stg_customer_360: DataFrame) -> DataFrame:
    def has(flag: str) -> F.Column:
        return F.coalesce(F.col(flag) == "Y", F.lit(False)).cast("double")

    tenure, age, balance = F.col("TENURE_MONTHS"), F.col("AGE"), F.col("TOTAL_BALANCE")
    return (
        stg_customer_360.where(F.col("CUSTOMER_STATUS") == "A")
        .select(
            F.col("CUSTOMER_ID").cast(LongType()).alias("CUSTOMER_ID"),
            F.col("AGE").cast("double").alias("AGE"),
            F.col("TENURE_MONTHS").cast("double").alias("TENURE_MONTHS"),
            F.col("TOTAL_BALANCE").cast("double").alias("TOTAL_BALANCE"),
            F.col("CREDIT_UTILIZATION_PCT")
            .cast("double")
            .alias("CREDIT_UTILIZATION_PCT"),
            F.col("NUM_ACCOUNTS").cast("double").alias("NUM_ACCOUNTS"),
            F.col("NUM_ACTIVE_ACCOUNTS").cast("double").alias("NUM_ACTIVE_ACCOUNTS"),
            (
                (
                    has("HAS_CHECKING")
                    + has("HAS_SAVINGS")
                    + has("HAS_CREDIT")
                    + has("HAS_LOAN")
                )
                / 4.0
            ).alias("PRODUCT_BREADTH"),
        )
        .withColumn(
            "TENURE_GROUP",
            F.when(_lt_or_missing(tenure, 12), "NEW (<1yr)")
            .when(tenure < 36, "DEVELOPING (1-3yr)")
            .when(tenure < 84, "ESTABLISHED (3-7yr)")
            .otherwise("LOYAL (7yr+)"),
        )
        .withColumn(
            "AGE_GROUP",
            F.when(_lt_or_missing(age, 25), "GEN_Z")
            .when(age < 41, "MILLENNIAL")
            .when(age < 57, "GEN_X")
            .when(age < 76, "BOOMER")
            .otherwise("SILENT"),
        )
        .withColumn(
            "BALANCE_TIER",
            F.when(_lt_or_missing(balance, 1000), "LOW")
            .when(balance < 10000, "MODERATE")
            .when(balance < 100000, "AFFLUENT")
            .otherwise("HIGH_NET_WORTH"),
        )
        # SAS max() ignores missing values, so a missing balance gives log(1) = 0.
        .withColumn("LOG_BALANCE", F.log(F.greatest(balance, F.lit(1.0))))
        .withColumn(
            "ACCT_RATIO",
            F.col("NUM_ACTIVE_ACCOUNTS")
            / F.greatest(F.col("NUM_ACCOUNTS"), F.lit(1.0)),
        )
    )


def _standardise(features: DataFrame) -> DataFrame:
    """PROC STDIZE METHOD=STD; adds the `_features` vector of standardised CLUSTER_FEATURES."""
    # PROC STDIZE takes location/scale from non-missing values only. FASTCLUS (without NOMISS)
    # still assigns rows with missing VARs, so missing standardised values become 0 (the mean).
    standardised = []
    for c in CLUSTER_FEATURES:
        observed = VectorAssembler(
            inputCols=[c], outputCol="_v", handleInvalid="skip"
        ).transform(features.select(c))
        stats = StandardScaler(
            inputCol="_v", outputCol="_z", withMean=True, withStd=True
        ).fit(observed)
        mean = float(stats.mean[0]) if stats.mean.size else 0.0
        std = float(stats.std[0]) if stats.std.size else 0.0
        z = (F.col(c) - F.lit(mean)) / F.lit(std) if std > 0 else F.lit(0.0)
        standardised.append(F.coalesce(z, F.lit(0.0)).alias(f"_z_{c}"))

    scaled = VectorAssembler(
        inputCols=[f"_z_{c}" for c in CLUSTER_FEATURES], outputCol="_features"
    ).transform(features.select("*", *standardised))
    return scaled.select(*features.columns, "_features")


def _cluster(features: DataFrame, n_init: int = N_INIT) -> DataFrame:
    """PROC STDIZE + PROC FASTCLUS; returns features plus CLUSTER (1..k, like FASTCLUS)."""
    scaled = _standardise(features)
    scaled.cache()
    try:
        model = _best_kmeans(scaled, n_init)
    finally:
        scaled.unpersist()
    return (
        model.transform(scaled)
        .withColumn("CLUSTER", (F.col("_pred") + 1).cast(ShortType()))
        .drop("_features", "_pred")
    )


def _best_kmeans(scaled: DataFrame, n_init: int) -> KMeansModel:
    best = None
    for seed in range(SEED, SEED + n_init):
        model = KMeans(
            k=N_CLUSTERS,
            maxIter=MAX_ITER,
            tol=CONVERGE,
            seed=seed,
            featuresCol="_features",
            predictionCol="_pred",
        ).fit(scaled)
        if best is None or model.summary.trainingCost < best.summary.trainingCost:
            best = model
    return best


def _segment_labels(clustered: DataFrame) -> DataFrame:
    """CLUSTER_PROFILES ordered by AVG_BALANCE desc, then SEGMENT_LABELS by rank (_N_)."""
    profiles = clustered.groupBy("CLUSTER").agg(
        F.avg("LOG_BALANCE").alias("AVG_BALANCE")
    )
    rank = F.row_number().over(
        Window.orderBy(F.col("AVG_BALANCE").desc(), F.col("CLUSTER"))
    )
    label = F.element_at(
        F.array(*[F.lit(s) for s in SEGMENT_LABELS]),
        F.least(rank, F.lit(len(SEGMENT_LABELS))),
    )
    return profiles.select(
        "CLUSTER",
        label.alias("SEGMENT_NAME"),
        F.lit(0).cast(ShortType()).alias("SUBSEGMENT_ID"),
    )


def transform(
    stg_customer_360: DataFrame, as_of_date: datetime.date, **params
) -> DataFrame:
    """Build dp.CUSTOMER_SEGMENTS from stg.STG_CUSTOMER_360 as of `as_of_date`."""
    clustered = _cluster(
        _engineer_features(stg_customer_360), n_init=int(params.get("n_init", N_INIT))
    )
    labelled = clustered.join(
        F.broadcast(_segment_labels(clustered)), "CLUSTER", "inner"
    )

    breadth, ratio = F.col("PRODUCT_BREADTH"), F.col("ACCT_RATIO")
    out = labelled.select(
        F.col("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col("CLUSTER").alias("SEGMENT_ID"),
        F.col("SUBSEGMENT_ID"),
        F.round(F.col("LOG_BALANCE") * F.col("TENURE_MONTHS") * breadth * 10, 2).alias(
            "LIFETIME_VALUE_SCORE"
        ),
        F.round(ratio * 100, 2).alias("ENGAGEMENT_SCORE"),
        F.lit(0).alias("DIGITAL_ADOPTION_SCORE"),
        F.round(breadth * 100, 2).alias("PRODUCT_BREADTH_INDEX"),
        F.col("TENURE_GROUP"),
        F.col("AGE_GROUP"),
        F.col("BALANCE_TIER"),
        # SAS '' is a missing character value; SAS/ACCESS loads it as NULL.
        F.lit(None).alias("CHANNEL_PREFERENCE"),
        F.when((breadth < 0.50) & (ratio >= 0.75), "Y")
        .otherwise("N")
        .alias("CROSS_SELL_FLAG"),
        F.when(
            (F.col("BALANCE_TIER") == "MODERATE")
            & (F.col("TENURE_GROUP") != "NEW (<1yr)"),
            "Y",
        )
        .otherwise("N")
        .alias("UPSELL_FLAG"),
        F.when(_lt_or_missing(ratio, 0.50) & (F.col("TENURE_MONTHS") >= 60), "Y")
        .otherwise("N")
        .alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.lit(as_of_date).alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return out.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in OUTPUT_SCHEMA.fields]
    )


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from synapse.spark.common import pipeline_utils as pu

    params = pu.get_run_params(argv)
    spark = SparkSession.builder.appName("sjd_customer_segments").getOrCreate()
    pu.log_step(
        spark,
        JOB_NAME,
        STEP_NAME,
        "START",
        msg=f"as_of_date={params.as_of_date} env={params.env}",
    )
    try:
        source = pu.read_table(spark, SOURCE_SCHEMA, SOURCE_TABLE)
        result = transform(source, params.as_of_date).cache()
    except Exception as exc:
        pu.log_step(spark, JOB_NAME, STEP_NAME, "FAILED", msg=str(exc)[:4000])
        raise
    try:
        min_rows = SAS_MIN_ROWS if params.env == "prod" else 1
        pu.validate_table(
            result, key_cols=KEY_COLS, not_null_cols=NOT_NULL_COLS, min_rows=min_rows
        )
        row_count = result.count()
        pu.write_table(result, TARGET_SCHEMA, TARGET_TABLE, mode="overwrite")
    except pu.ValidationError as exc:
        pu.log_step(
            spark,
            JOB_NAME,
            STEP_NAME,
            "FAILED",
            msg=f"Validation failed - aborting load: {exc}"[:4000],
        )
        return 1
    except Exception as exc:
        pu.log_step(spark, JOB_NAME, STEP_NAME, "FAILED", msg=str(exc)[:4000])
        raise
    finally:
        result.unpersist()
    pu.log_step(
        spark,
        JOB_NAME,
        STEP_NAME,
        "SUCCESS",
        row_count=row_count,
        msg="Pipeline complete",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
