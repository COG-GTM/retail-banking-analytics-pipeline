"""Data-product job 01 -- CUSTOMER_SEGMENTS.

Faithful PySpark port of ``sas/01_sas_customer_segments.sas``: read the
BTEQ-produced ``STG_CUSTOMER_360`` staging table (active customers only),
engineer clustering features, run k-means to segment customers, label the
clusters with business-meaningful names, compute the value/engagement scores and
action flags, and write the ``CUSTOMER_SEGMENTS`` data product.

SAS -> PySpark mapping:
* ``PROC SQL ... where CUSTOMER_STATUS = 'A'`` -> ``read_staging`` + filter in
  :func:`engineer_features`.
* SAS ``DATA`` step feature engineering (``PRODUCT_BREADTH``, ``TENURE_GROUP``,
  ``AGE_GROUP``, ``BALANCE_TIER``, ``LOG_BALANCE``, ``ACCT_RATIO``) ->
  :func:`engineer_features` column expressions.
* ``PROC STDIZE method=std`` + ``PROC FASTCLUS maxclusters=5 maxiter=50
  converge=0.001`` -> :class:`VectorAssembler` + :class:`StandardScaler`
  (``withMean``/``withStd`` = STDIZE center + unit-std) + :class:`KMeans`
  (fixed seed) in :func:`cluster_customers`.
* Cluster labelling ``order by AVG_BALANCE desc`` + ``_N_`` rank ->
  :func:`segment_labels` (``row_number`` over avg-log-balance DESC).
* SAS ``STEP 6`` scores/flags -> :func:`build_output`.

Fidelity notes (see PR body): KMeans centroids/assignments are non-deterministic
vs SAS FASTCLUS, so cluster parity is tolerance-based, not exact. The labelling
rule (ordered by avg balance) and all derived flags/scores are deterministic
given the cluster assignments. Unlike the literal SAS -- which reused the
STDIZE-standardised columns for the STEP 5/6 averages, scores and flags -- this
port uses the *raw* engineered features for labelling, scoring and flags
(scaling is confined to the clustering vector), as specified by the migration
task.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.dates import load_timestamp
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table

JOB_NAME = "01_customer_segments"
TARGET = "CUSTOMER_SEGMENTS"
MODEL_VERSION = "SEG_V3.2"

# Clustering feature order (SAS VAR list on PROC STDIZE / PROC FASTCLUS).
FEATURE_COLS = (
    "log_balance",
    "tenure_months",
    "credit_utilization_pct",
    "product_breadth",
    "acct_ratio",
    "age",
)

# Fixed seed so KMeans assignments are reproducible across runs.
KMEANS_SEED = 42

# Segment names in descending average-balance order (SAS STEP 5 _N_ ladder).
SEGMENT_NAMES = (
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
)


def engineer_features(stg: DataFrame, config: PipelineConfig) -> DataFrame:
    """Engineer the clustering features for active customers (SAS STEP 1-2).

    Filters ``STG_CUSTOMER_360`` to ``customer_status = 'A'`` and derives the
    product-breadth index, tenure/age/balance groupings, digital-adoption
    placeholder, log balance and active-account ratio.
    """
    def _flag(col: str) -> F.Column:
        return F.when(F.col(col) == "Y", 1).otherwise(0)

    product_breadth = (
        _flag("has_checking") + _flag("has_savings") + _flag("has_credit") + _flag("has_loan")
    ) / F.lit(4.0)

    tenure_group = (
        F.when(F.col("tenure_months") < 12, "NEW (<1yr)")
        .when(F.col("tenure_months") < 36, "DEVELOPING (1-3yr)")
        .when(F.col("tenure_months") < 84, "ESTABLISHED (3-7yr)")
        .otherwise("LOYAL (7yr+)")
    )

    age_group = (
        F.when(F.col("age") < 25, "GEN_Z")
        .when(F.col("age") < 41, "MILLENNIAL")
        .when(F.col("age") < 57, "GEN_X")
        .when(F.col("age") < 76, "BOOMER")
        .otherwise("SILENT")
    )

    balance_tier = (
        F.when(F.col("total_balance") < 1000, "LOW")
        .when(F.col("total_balance") < 10000, "MODERATE")
        .when(F.col("total_balance") < 100000, "AFFLUENT")
        .otherwise("HIGH_NET_WORTH")
    )

    log_balance = F.log(F.greatest(F.col("total_balance").cast("double"), F.lit(1.0)))
    acct_ratio = F.coalesce(F.col("num_active_accounts").cast("double"), F.lit(0.0)) / F.greatest(
        F.col("num_accounts").cast("double"), F.lit(1.0)
    )

    return (
        stg
        .filter(F.col("customer_status") == "A")
        .withColumn("product_breadth", product_breadth.cast("double"))
        .withColumn("tenure_group", tenure_group)
        .withColumn("age_group", age_group)
        .withColumn("balance_tier", balance_tier)
        .withColumn("digital_adoption_score", F.lit(0).cast("double"))
        .withColumn("log_balance", log_balance)
        .withColumn("acct_ratio", acct_ratio)
    )


def cluster_customers(features: DataFrame, seed: int = KMEANS_SEED) -> DataFrame:
    """Standardise the feature vector and assign a KMeans cluster (SAS STEP 3-4).

    ``VectorAssembler`` -> ``StandardScaler(withMean, withStd)`` (PROC STDIZE
    method=std) -> ``KMeans(k=5, maxIter=50, tol=0.001)`` (PROC FASTCLUS). The
    raw feature columns are left untouched for downstream labelling/scoring;
    only the assembled vector is scaled. Returns ``features`` with an added
    integer ``cluster`` column.
    """
    assemble_cols = []
    prepared = features
    for col in FEATURE_COLS:
        tmp = f"_asm_{col}"
        prepared = prepared.withColumn(tmp, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))
        assemble_cols.append(tmp)

    assembler = VectorAssembler(inputCols=assemble_cols, outputCol="_feature_vec")
    scaler = StandardScaler(
        inputCol="_feature_vec", outputCol="_scaled_vec", withMean=True, withStd=True
    )
    kmeans = KMeans(
        featuresCol="_scaled_vec",
        predictionCol="cluster",
        k=5,
        maxIter=50,
        tol=0.001,
        seed=seed,
    )

    assembled = assembler.transform(prepared)
    scaled = scaler.fit(assembled).transform(assembled)
    clustered = kmeans.fit(scaled).transform(scaled)
    drop_cols = [*assemble_cols, "_feature_vec", "_scaled_vec"]
    return clustered.drop(*drop_cols)


def segment_labels(clustered: DataFrame) -> DataFrame:
    """Label clusters by descending average log balance (SAS STEP 5).

    Returns one row per cluster: ``cluster``, ``segment_name`` (assigned by the
    avg-log-balance rank ladder) and ``subsegment_id`` (0, placeholder).
    """
    profiles = clustered.groupBy("cluster").agg(F.avg("log_balance").alias("avg_balance"))
    rank_win = Window.orderBy(F.col("avg_balance").desc(), F.col("cluster").asc())
    ranked = profiles.withColumn("_rn", F.row_number().over(rank_win))

    name_expr = F.lit(SEGMENT_NAMES[-1])
    for idx, name in enumerate(SEGMENT_NAMES[:-1], start=1):
        name_expr = F.when(F.col("_rn") == idx, F.lit(name)).otherwise(name_expr)

    return (
        ranked
        .withColumn("segment_name", name_expr)
        .withColumn("subsegment_id", F.lit(0))
        .select("cluster", "segment_name", "subsegment_id")
    )


def build_output(labelled: DataFrame, config: PipelineConfig) -> DataFrame:
    """Compute scores + action flags and enforce the DDL contract (SAS STEP 6).

    ``labelled`` must carry the engineered features plus ``cluster``,
    ``segment_name`` and ``subsegment_id``.
    """
    lifetime_value_score = F.round(
        F.col("log_balance") * F.col("tenure_months") * F.col("product_breadth") * F.lit(10), 2
    )
    engagement_score = F.round(F.col("acct_ratio") * F.lit(100), 2)
    product_breadth_index = F.round(F.col("product_breadth") * F.lit(100), 2)

    cross_sell_flag = F.when(
        (F.col("product_breadth") < 0.50) & (F.col("acct_ratio") >= 0.75), "Y"
    ).otherwise("N")
    upsell_flag = F.when(
        (F.col("balance_tier") == "MODERATE") & (F.col("tenure_group") != "NEW (<1yr)"), "Y"
    ).otherwise("N")
    retention_risk_flag = F.when(
        (F.col("acct_ratio") < 0.50) & (F.col("tenure_months") >= 60), "Y"
    ).otherwise("N")

    df = (
        labelled
        .withColumn("segment_id", F.col("cluster"))
        .withColumn("lifetime_value_score", lifetime_value_score)
        .withColumn("engagement_score", engagement_score)
        .withColumn("product_breadth_index", product_breadth_index)
        .withColumn("channel_preference", F.lit(""))
        .withColumn("cross_sell_flag", cross_sell_flag)
        .withColumn("upsell_flag", upsell_flag)
        .withColumn("retention_risk_flag", retention_risk_flag)
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.lit(config.run_date))
        .withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    )
    return schemas.enforce_schema(df, schemas.CUSTOMER_SEGMENTS)


def transform(stg: DataFrame, config: PipelineConfig, seed: int = KMEANS_SEED) -> DataFrame:
    """Full CUSTOMER_SEGMENTS transform: features -> cluster -> label -> scores."""
    features = engineer_features(stg, config)
    clustered = cluster_customers(features, seed)
    labels = segment_labels(clustered)
    labelled = clustered.join(F.broadcast(labels), "cluster", "inner")
    return build_output(labelled, config)


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read STG_CUSTOMER_360, segment customers, validate, and write the product."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", "Beginning customer segmentation pipeline")

    stg = io.read_staging("STG_CUSTOMER_360")

    out = transform(stg, config).cache()
    n = out.count()

    result = validate_table(
        out,
        TARGET,
        key_cols=["customer_id"],
        not_null=["customer_id", "segment_name", "segment_id"],
        min_rows=1,
        audit=audit,
    )
    abort_on_failure(result)

    io.write_data_product(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Data product written", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build CUSTOMER_SEGMENTS")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    parser.add_argument(
        "--read-products-from-source",
        action="store_true",
        help="Read staging/products from the committed CSVs instead of the lake.",
    )
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(
        spark,
        config,
        args.source_dir,
        args.lake_dir,
        read_products_from_source=args.read_products_from_source,
    )
    run(spark, io, config)


if __name__ == "__main__":
    main()
