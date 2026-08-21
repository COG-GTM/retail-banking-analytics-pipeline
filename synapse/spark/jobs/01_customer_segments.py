"""Synapse Spark job: customer segmentation (migrated from sas/01_sas_customer_segments.sas).

Reads ETL_STAGING.STG_CUSTOMER_360 from Snowflake, engineers the clustering
features, standardizes them, runs k-means (k=5), labels the clusters with the
business segment names and writes DATA_PRODUCTS.CUSTOMER_SEGMENTS back to
Snowflake with the Snowflake Spark connector.

SAS -> PySpark mapping:
    PROC SQL extract      -> Snowflake connector read with the CUSTOMER_STATUS='A' predicate
    DATA step features    -> engineer_features()
    PROC STDIZE method=std-> pyspark.ml.feature.StandardScaler(withMean=True, withStd=True)
    PROC FASTCLUS k=5     -> pyspark.ml.clustering.KMeans(k=5, maxIter=50, tol=0.001, seed=42)
    Label/score PROC SQL  -> label_clusters() + build_customer_segments()
    %validate_table       -> validate_output()
    PROC APPEND (truncate/load) -> write_customer_segments() with mode="overwrite"

Local reconciliation run (no Snowflake required):

    python synapse/spark/jobs/01_customer_segments.py \
        --input-csv data/02_bteq_staging/stg_customer_360.csv \
        --output-csv /tmp/customer_segments_spark --min-rows 100
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOG = logging.getLogger("customer_segments")

MODEL_VERSION = "SEG_V3.2"
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

CLUSTER_FEATURES = [
    "LOG_BALANCE",
    "TENURE_MONTHS",
    "CREDIT_UTILIZATION_PCT",
    "PRODUCT_BREADTH",
    "ACCT_RATIO",
    "AGE",
]

# Cluster profiles are ordered by descending mean LOG_BALANCE, exactly as the
# SAS `order by AVG_BALANCE desc` / `_N_` labelling in WORK.SEGMENT_LABELS.
SEGMENT_LABELS = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]

K_CLUSTERS = 5
MAX_ITER = 50
TOLERANCE = 0.001
INIT_STEPS = 5
DEFAULT_SEED = 42
N_INIT = 20  # deterministic restarts; the lowest-cost fit is kept

STG_COLUMNS = [
    "CUSTOMER_ID",
    "AGE",
    "TENURE_MONTHS",
    "CUSTOMER_STATUS",
    "SEGMENT_CODE",
    "STATE_CODE",
    "NUM_ACCOUNTS",
    "NUM_ACTIVE_ACCOUNTS",
    "HAS_CHECKING",
    "HAS_SAVINGS",
    "HAS_CREDIT",
    "HAS_LOAN",
    "TOTAL_BALANCE",
    "TOTAL_CREDIT_LIMIT",
    "CREDIT_UTILIZATION_PCT",
]

OUTPUT_COLUMNS = [
    "CUSTOMER_ID",
    "SEGMENT_NAME",
    "SEGMENT_ID",
    "SUBSEGMENT_ID",
    "LIFETIME_VALUE_SCORE",
    "ENGAGEMENT_SCORE",
    "DIGITAL_ADOPTION_SCORE",
    "PRODUCT_BREADTH_INDEX",
    "TENURE_GROUP",
    "AGE_GROUP",
    "BALANCE_TIER",
    "CHANNEL_PREFERENCE",
    "CROSS_SELL_FLAG",
    "UPSELL_FLAG",
    "RETENTION_RISK_FLAG",
    "MODEL_VERSION",
    "EFFECTIVE_DATE",
    "LOAD_TS",
]

NOT_NULL_COLUMNS = ["CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"]


class ValidationError(RuntimeError):
    """Raised when the output fails a pre-publish check (SAS %ABORT CANCEL)."""


@dataclass(frozen=True)
class SnowflakeConfig:
    url: str
    user: str
    role: str
    warehouse: str
    database: str
    staging_schema: str
    data_product_schema: str
    private_key: str

    def options(self, schema: str) -> dict[str, str]:
        return {
            "sfURL": self.url,
            "sfUser": self.user,
            "sfRole": self.role,
            "sfWarehouse": self.warehouse,
            "sfDatabase": self.database,
            "sfSchema": schema,
            "pem_private_key": self.private_key,
        }


def _keyvault_secret(vault: str, secret_name: str) -> str:
    """Resolve a secret from Azure Key Vault (TICKET-02 credential model)."""
    from notebookutils import mssparkutils  # available on Synapse Spark pools

    return mssparkutils.credentials.getSecret(vault, secret_name)


def snowflake_config_from_env() -> SnowflakeConfig:
    """Build the Snowflake connection config from Synapse pipeline parameters.

    Credentials are never read from the repository: the key-pair private key
    comes from Azure Key Vault via the Synapse-managed identity.
    """
    return SnowflakeConfig(
        url=os.environ["SNOWFLAKE_URL"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        staging_schema=os.environ.get("SNOWFLAKE_STAGING_SCHEMA", "ETL_STAGING"),
        data_product_schema=os.environ.get("SNOWFLAKE_DP_SCHEMA", "DATA_PRODUCTS"),
        private_key=_keyvault_secret(
            os.environ["AZURE_KEY_VAULT_NAME"],
            os.environ.get("SNOWFLAKE_PRIVATE_KEY_SECRET", "snowflake-spark-private-key"),
        ),
    )


def read_stg_customer_360(spark: SparkSession, config: SnowflakeConfig) -> DataFrame:
    """Read the active customers from STG_CUSTOMER_360 (SAS STEP 1)."""
    query = (
        f"SELECT {', '.join(STG_COLUMNS)} "
        f"FROM {config.database}.{config.staging_schema}.STG_CUSTOMER_360 "
        "WHERE CUSTOMER_STATUS = 'A'"
    )
    return (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**config.options(config.staging_schema))
        .option("query", query)
        .load()
    )


def read_stg_customer_360_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read STG_CUSTOMER_360 from a CSV extract (offline reconciliation runs)."""
    raw = spark.read.option("header", True).option("inferSchema", True).csv(path)
    renamed = raw.select([F.col(c).alias(c.upper()) for c in raw.columns])
    return renamed.select(*STG_COLUMNS).where(F.col("CUSTOMER_STATUS") == "A")


def engineer_features(df: DataFrame) -> DataFrame:
    """Port of the WORK.CUST_FEATURES data step (SAS STEP 2)."""
    held = [
        (F.col(c) == F.lit("Y")).cast("double")
        for c in ("HAS_CHECKING", "HAS_SAVINGS", "HAS_CREDIT", "HAS_LOAN")
    ]
    product_breadth = sum(held[1:], held[0]) / F.lit(4.0)

    tenure_group = (
        F.when(F.col("TENURE_MONTHS") < 12, "NEW (<1yr)")
        .when(F.col("TENURE_MONTHS") < 36, "DEVELOPING (1-3yr)")
        .when(F.col("TENURE_MONTHS") < 84, "ESTABLISHED (3-7yr)")
        .otherwise("LOYAL (7yr+)")
    )
    age_group = (
        F.when(F.col("AGE") < 25, "GEN_Z")
        .when(F.col("AGE") < 41, "MILLENNIAL")
        .when(F.col("AGE") < 57, "GEN_X")
        .when(F.col("AGE") < 76, "BOOMER")
        .otherwise("SILENT")
    )
    balance_tier = (
        F.when(F.col("TOTAL_BALANCE") < 1000, "LOW")
        .when(F.col("TOTAL_BALANCE") < 10000, "MODERATE")
        .when(F.col("TOTAL_BALANCE") < 100000, "AFFLUENT")
        .otherwise("HIGH_NET_WORTH")
    )

    return (
        df.withColumn("PRODUCT_BREADTH", product_breadth)
        .withColumn("TENURE_GROUP", tenure_group)
        .withColumn("AGE_GROUP", age_group)
        .withColumn("BALANCE_TIER", balance_tier)
        # Placeholder, enriched later by the transaction analytics product.
        .withColumn("DIGITAL_ADOPTION_SCORE", F.lit(0.0))
        .withColumn("LOG_BALANCE", F.log(F.greatest(F.col("TOTAL_BALANCE"), F.lit(1.0))))
        .withColumn(
            "ACCT_RATIO",
            F.col("NUM_ACTIVE_ACCOUNTS").cast("double")
            / F.greatest(F.col("NUM_ACCOUNTS").cast("double"), F.lit(1.0)),
        )
        .withColumn("TENURE_MONTHS", F.col("TENURE_MONTHS").cast("double"))
        .withColumn("AGE", F.col("AGE").cast("double"))
        .withColumn("CREDIT_UTILIZATION_PCT", F.col("CREDIT_UTILIZATION_PCT").cast("double"))
    )


def assign_clusters(df: DataFrame, seed: int = DEFAULT_SEED, n_init: int = N_INIT) -> DataFrame:
    """Standardize the clustering features and fit k-means (SAS STEPS 3-4).

    StandardScaler(withMean=True, withStd=True) reproduces PROC STDIZE
    METHOD=STD (subtract the mean, divide by the corrected sample standard
    deviation). Unlike PROC STDIZE the scaling is done out-of-place, so the raw
    feature values stay available for the downstream scores.

    The feature space has several near-equal local optima, so the model is
    refitted from `n_init` seeds derived from `seed` and the lowest-cost fit
    wins - the deterministic equivalent of PROC FASTCLUS REPLACE=FULL, which
    also rebuilds its seeds during the first pass.
    """
    assembled = VectorAssembler(
        inputCols=CLUSTER_FEATURES, outputCol="RAW_FEATURES", handleInvalid="skip"
    ).transform(df)
    scaler = StandardScaler(
        inputCol="RAW_FEATURES", outputCol="SCALED_FEATURES", withMean=True, withStd=True
    ).fit(assembled)
    scaled = scaler.transform(assembled).cache()

    best_model, best_cost = None, None
    for offset in range(n_init):
        model = KMeans(
            featuresCol="SCALED_FEATURES",
            predictionCol="CLUSTER",
            k=K_CLUSTERS,
            maxIter=MAX_ITER,
            tol=TOLERANCE,
            seed=seed + offset,
            initMode="k-means||",
            initSteps=INIT_STEPS,
        ).fit(scaled)
        cost = model.summary.trainingCost
        LOG.info("k-means fit seed=%s cost=%.4f", seed + offset, cost)
        if best_cost is None or cost < best_cost:
            best_model, best_cost = model, cost

    LOG.info("selected k-means fit with cost %.4f", best_cost)
    clustered = best_model.transform(scaled)
    return clustered.drop("RAW_FEATURES", "SCALED_FEATURES")


def label_clusters(clustered: DataFrame) -> DataFrame:
    """Map cluster ids to business segment names (SAS STEP 5).

    Clusters are ranked by descending mean LOG_BALANCE, which makes the labels
    invariant to the arbitrary cluster numbering produced by k-means.
    """
    profiles = clustered.groupBy("CLUSTER").agg(
        F.count(F.lit(1)).alias("N"),
        F.avg("LOG_BALANCE").alias("AVG_BALANCE"),
        F.avg("TENURE_MONTHS").alias("AVG_TENURE"),
        F.avg("PRODUCT_BREADTH").alias("AVG_BREADTH"),
        F.avg("CREDIT_UTILIZATION_PCT").alias("AVG_CREDIT_UTIL"),
    )
    ranked = profiles.withColumn(
        "PROFILE_RANK",
        F.row_number().over(Window.orderBy(F.col("AVG_BALANCE").desc(), F.col("CLUSTER"))),
    )
    label_expr = F.lit(SEGMENT_LABELS[-1])
    for rank, name in reversed(list(enumerate(SEGMENT_LABELS, start=1))):
        label_expr = F.when(F.col("PROFILE_RANK") == rank, F.lit(name)).otherwise(label_expr)

    return ranked.select(
        "CLUSTER",
        label_expr.alias("SEGMENT_NAME"),
        F.lit(0).cast("smallint").alias("SUBSEGMENT_ID"),
    )


def build_customer_segments(clustered: DataFrame, labels: DataFrame) -> DataFrame:
    """Score, flag and shape the CUSTOMER_SEGMENTS data product (SAS STEP 6)."""
    joined = clustered.join(labels, on="CLUSTER", how="inner")

    lifetime_value = F.round(
        F.col("LOG_BALANCE") * F.col("TENURE_MONTHS") * F.col("PRODUCT_BREADTH") * F.lit(10), 2
    )
    engagement = F.round(F.col("ACCT_RATIO") * F.lit(100), 2)
    breadth_index = F.round(F.col("PRODUCT_BREADTH") * F.lit(100), 2)

    cross_sell = F.when(
        (F.col("PRODUCT_BREADTH") < 0.50) & (F.col("ACCT_RATIO") >= 0.75), "Y"
    ).otherwise("N")
    upsell = F.when(
        (F.col("BALANCE_TIER") == "MODERATE") & (F.col("TENURE_GROUP") != "NEW (<1yr)"), "Y"
    ).otherwise("N")
    retention_risk = F.when(
        (F.col("ACCT_RATIO") < 0.50) & (F.col("TENURE_MONTHS") >= 60), "Y"
    ).otherwise("N")

    return joined.select(
        F.col("CUSTOMER_ID").cast("bigint").alias("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col("CLUSTER").cast("smallint").alias("SEGMENT_ID"),
        F.col("SUBSEGMENT_ID"),
        lifetime_value.cast("decimal(10,2)").alias("LIFETIME_VALUE_SCORE"),
        engagement.cast("decimal(5,2)").alias("ENGAGEMENT_SCORE"),
        F.col("DIGITAL_ADOPTION_SCORE").cast("decimal(5,2)").alias("DIGITAL_ADOPTION_SCORE"),
        breadth_index.cast("decimal(5,2)").alias("PRODUCT_BREADTH_INDEX"),
        F.col("TENURE_GROUP"),
        F.col("AGE_GROUP"),
        F.col("BALANCE_TIER"),
        F.lit("").alias("CHANNEL_PREFERENCE"),
        cross_sell.alias("CROSS_SELL_FLAG"),
        upsell.alias("UPSELL_FLAG"),
        retention_risk.alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.current_date().alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    ).select(*OUTPUT_COLUMNS)


def validate_output(df: DataFrame, min_rows: int) -> int:
    """Row count, uniqueness and null-rate checks (SAS %validate_table)."""
    counts = df.agg(
        F.count(F.lit(1)).alias("ROWS"),
        F.countDistinct("CUSTOMER_ID").alias("DISTINCT_KEYS"),
        *[F.sum(F.col(c).isNull().cast("long")).alias(f"NULLS_{c}") for c in NOT_NULL_COLUMNS],
    ).collect()[0]

    rows = counts["ROWS"]
    failures = []
    if rows < min_rows:
        failures.append(f"row count {rows} below minimum {min_rows}")
    if counts["DISTINCT_KEYS"] != rows:
        failures.append(f"CUSTOMER_ID is not unique ({counts['DISTINCT_KEYS']} distinct of {rows})")
    for column in NOT_NULL_COLUMNS:
        nulls = counts[f"NULLS_{column}"]
        if nulls:
            failures.append(f"{column} has {nulls} null(s) ({nulls / rows:.2%})")

    if failures:
        raise ValidationError("CUSTOMER_SEGMENTS validation failed: " + "; ".join(failures))

    LOG.info("Validation passed: %s rows, unique CUSTOMER_ID, no nulls in %s", rows, NOT_NULL_COLUMNS)
    return rows


def write_customer_segments(df: DataFrame, config: SnowflakeConfig) -> None:
    """Truncate-and-load CUSTOMER_SEGMENTS (SAS DELETE + PROC APPEND)."""
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**config.options(config.data_product_schema))
        .option("dbtable", "CUSTOMER_SEGMENTS")
        .option("truncate_table", "on")
        .option("usestagingtable", "off")
        .mode("overwrite")
        .save()
    )


def run(
    spark: SparkSession,
    source: DataFrame,
    seed: int = DEFAULT_SEED,
    min_rows: int = 1000,
    n_init: int = N_INIT,
) -> DataFrame:
    """Feature engineering -> clustering -> labelling -> validation."""
    features = engineer_features(source).cache()
    clustered = assign_clusters(features, seed=seed, n_init=n_init)
    labels = label_clusters(clustered)
    segments = build_customer_segments(clustered, labels).cache()
    validate_output(segments, min_rows=min_rows)
    return segments


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Customer segmentation Synapse Spark job")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="k-means seed")
    parser.add_argument(
        "--min-rows", type=int, default=1000, help="minimum output rows before publish"
    )
    parser.add_argument(
        "--n-init", type=int, default=N_INIT, help="deterministic k-means restarts"
    )
    parser.add_argument("--input-csv", help="read STG_CUSTOMER_360 from CSV instead of Snowflake")
    parser.add_argument("--output-csv", help="write the product to CSV instead of Snowflake")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)

    spark = SparkSession.builder.appName("01_customer_segments").getOrCreate()
    config = None if args.input_csv and args.output_csv else snowflake_config_from_env()

    if args.input_csv:
        source = read_stg_customer_360_csv(spark, args.input_csv)
    else:
        source = read_stg_customer_360(spark, config)

    segments = run(spark, source, seed=args.seed, min_rows=args.min_rows, n_init=args.n_init)

    if args.output_csv:
        segments.coalesce(1).write.option("header", True).mode("overwrite").csv(args.output_csv)
    else:
        write_customer_segments(segments, config)

    LOG.info("CUSTOMER_SEGMENTS published (model version %s)", MODEL_VERSION)
    return 0


if __name__ == "__main__":
    sys.exit(main())
