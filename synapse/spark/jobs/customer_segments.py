"""Customer segmentation on Azure Synapse Spark.

PySpark replacement for ``sas/01_sas_customer_segments.sas`` (MBA-2207 / TICKET-06).

Reads ``STG_CUSTOMER_360`` from Snowflake, engineers the segmentation features,
standardises them, runs k-means (k=5), labels the clusters, derives the LTV /
engagement / action-flag columns and publishes ``CUSTOMER_SEGMENTS`` back to
Snowflake.

SAS -> Spark mapping
--------------------
========================  ==================================================
SAS construct             Spark equivalent
========================  ==================================================
LIBNAME STGDB (Teradata)  Snowflake Spark connector read
PROC SQL WHERE            DataFrame filter on CUSTOMER_STATUS = 'A'
DATA step features        ``engineer_features``
PROC STDIZE METHOD=STD    ``StandardScaler(withMean=True, withStd=True)``
PROC FASTCLUS LEAST=2     ``pyspark.ml.clustering.KMeans`` (Euclidean)
PROC SQL cluster profile  ``label_segments`` (rank clusters by mean balance)
%validate_table           ``validate``
PROC APPEND to DPDB       Snowflake overwrite write
========================  ==================================================

Run locally (unit tests / reconciliation) or as a Synapse Spark job definition;
see ``docs/modernization/synapse/06-customer-segments.md``.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOGGER = logging.getLogger("customer_segments")

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

MODEL_VERSION = "SEG_V3.2"
NUM_CLUSTERS = 5
MAX_ITER = 50
TOLERANCE = 0.001
DEFAULT_SEED = 20260401
MIN_OUTPUT_ROWS = 1000

#: Features fed to PROC STDIZE / PROC FASTCLUS, in the SAS VAR statement order.
CLUSTER_FEATURES = [
    "LOG_BALANCE",
    "TENURE_MONTHS",
    "CREDIT_UTILIZATION_PCT",
    "PRODUCT_BREADTH",
    "ACCT_RATIO",
    "AGE",
]

#: Cluster labels applied in descending order of standardised mean balance,
#: mirroring the ``_N_``-based labelling in the SAS DATA step.
SEGMENT_LABELS = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]

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


class ValidationError(RuntimeError):
    """Raised when the output fails a pre-publish quality gate."""


@dataclass(frozen=True)
class SnowflakeConfig:
    """Connection settings for the Snowflake Spark connector.

    Database and schema names follow the layout defined by TICKET-01/TICKET-02;
    every value is overridable on the command line so no naming assumption is
    baked into the job.
    """

    url: str
    user: str
    role: str
    warehouse: str
    staging_database: str
    staging_schema: str
    product_database: str
    product_schema: str
    private_key: str | None = None
    password: str | None = None

    def options(self, database: str, schema: str) -> dict[str, str]:
        opts = {
            "sfUrl": self.url,
            "sfUser": self.user,
            "sfRole": self.role,
            "sfWarehouse": self.warehouse,
            "sfDatabase": database,
            "sfSchema": schema,
        }
        if self.private_key:
            opts["pem_private_key"] = self.private_key
        elif self.password:
            opts["sfPassword"] = self.password
        return opts

    def read_options(self) -> dict[str, str]:
        return self.options(self.staging_database, self.staging_schema)

    def write_options(self) -> dict[str, str]:
        return self.options(self.product_database, self.product_schema)


def _round2(col: Column) -> Column:
    """SAS ``round(x, 0.01)`` -> half-up rounding to two decimals."""
    return F.round(col.cast("double"), 2)


def read_staging(spark: SparkSession, config: SnowflakeConfig, table: str) -> DataFrame:
    """STEP 1 - pull STG_CUSTOMER_360 (active customers only)."""
    df = (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**config.read_options())
        .option("dbtable", table)
        .load()
    )
    return df.select(*STG_COLUMNS).where(F.col("CUSTOMER_STATUS") == F.lit("A"))


def engineer_features(df: DataFrame) -> DataFrame:
    """STEP 2 - reproduce the SAS feature-engineering DATA step."""
    product_flags = [
        (F.col(c) == F.lit("Y")).cast("double")
        for c in ("HAS_CHECKING", "HAS_SAVINGS", "HAS_CREDIT", "HAS_LOAN")
    ]
    product_breadth = sum(product_flags[1:], product_flags[0]) / F.lit(4.0)

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
        # Placeholder retained from SAS; enriched downstream by txn analytics.
        .withColumn("DIGITAL_ADOPTION_SCORE", F.lit(0.0))
        .withColumn(
            "LOG_BALANCE",
            F.log(F.greatest(F.col("TOTAL_BALANCE").cast("double"), F.lit(1.0))),
        )
        .withColumn(
            "ACCT_RATIO",
            F.col("NUM_ACTIVE_ACCOUNTS").cast("double")
            / F.greatest(F.col("NUM_ACCOUNTS").cast("double"), F.lit(1.0)),
        )
    )


def standardise(df: DataFrame) -> DataFrame:
    """STEP 3 - PROC STDIZE METHOD=STD equivalent.

    ``StandardScaler`` centres on the mean and divides by the corrected sample
    standard deviation, matching SAS. The scaled values replace the raw feature
    columns (as PROC STDIZE does with ``OUT=``); raw values are preserved with a
    ``RAW_`` prefix so downstream rules can use either.
    """
    features = df
    for column in CLUSTER_FEATURES:
        features = features.withColumn(
            f"RAW_{column}", F.col(column).cast("double")
        ).withColumn(column, F.col(column).cast("double"))

    assembler = VectorAssembler(
        inputCols=CLUSTER_FEATURES, outputCol="_raw_features", handleInvalid="skip"
    )
    scaler = StandardScaler(
        inputCol="_raw_features",
        outputCol="FEATURES",
        withMean=True,
        withStd=True,
    )
    assembled = assembler.transform(features)
    scaled = scaler.fit(assembled).transform(assembled).drop("_raw_features")

    to_array = vector_to_array(F.col("FEATURES"))
    for index, column in enumerate(CLUSTER_FEATURES):
        scaled = scaled.withColumn(column, to_array.getItem(index))
    return scaled


def cluster(df: DataFrame, seed: int = DEFAULT_SEED, k: int = NUM_CLUSTERS) -> DataFrame:
    """STEP 4 - PROC FASTCLUS (LEAST=2, MAXITER=50, CONVERGE=0.001) equivalent."""
    kmeans = KMeans(
        featuresCol="FEATURES",
        predictionCol="_CLUSTER0",
        k=k,
        maxIter=MAX_ITER,
        tol=TOLERANCE,
        distanceMeasure="euclidean",
        seed=seed,
    )
    clustered = kmeans.fit(df).transform(df)
    # SAS numbers clusters 1..k; Spark numbers them 0..k-1.
    return clustered.withColumn("CLUSTER", F.col("_CLUSTER0") + F.lit(1)).drop("_CLUSTER0")


def label_segments(df: DataFrame) -> DataFrame:
    """STEP 5 - rank clusters by mean standardised balance and label them."""
    profiles = df.groupBy("CLUSTER").agg(
        F.count(F.lit(1)).alias("N"),
        F.avg("LOG_BALANCE").alias("AVG_BALANCE"),
        F.avg("TENURE_MONTHS").alias("AVG_TENURE"),
        F.avg("PRODUCT_BREADTH").alias("AVG_BREADTH"),
        F.avg("CREDIT_UTILIZATION_PCT").alias("AVG_CREDIT_UTIL"),
    )
    # Deterministic ordering: mean balance desc, cluster id asc as tie-breaker.
    ranked = profiles.withColumn(
        "_RANK",
        F.row_number().over(
            Window.orderBy(F.col("AVG_BALANCE").desc(), F.col("CLUSTER").asc())
        ),
    )

    label_expr = F.lit(SEGMENT_LABELS[-1])
    for index, name in reversed(list(enumerate(SEGMENT_LABELS[:-1], start=1))):
        label_expr = F.when(F.col("_RANK") == index, F.lit(name)).otherwise(label_expr)

    return ranked.select(
        "CLUSTER",
        label_expr.alias("SEGMENT_NAME"),
        F.lit(0).cast("smallint").alias("SUBSEGMENT_ID"),
    )


def build_output(
    clustered: DataFrame, labels: DataFrame, score_basis: str = "raw"
) -> DataFrame:
    """STEP 6 - scores and action flags from the SAS PROC SQL.

    ``score_basis`` controls which copy of LOG_BALANCE / TENURE_MONTHS /
    PRODUCT_BREADTH / ACCT_RATIO feeds LIFETIME_VALUE_SCORE, ENGAGEMENT_SCORE
    and the cross-sell / retention thresholds:

    * ``"raw"`` (default) - pre-standardisation values. This reproduces the
      certified CUSTOMER_SEGMENTS output (engagement 0-100, positive LTV) and
      the DECIMAL(5,2) / DECIMAL(10,2) domains in the data product DDL.
    * ``"standardised"`` - the literal SAS expression, which references the
      PROC STDIZE output aliased as ``c.`` and therefore multiplies z-scores.

    PRODUCT_BREADTH_INDEX, the tier/group columns and the UPSELL rule always
    use raw values, exactly as in SAS (aliased ``f.``).
    """
    if score_basis not in ("raw", "standardised"):
        raise ValueError(f"unknown score_basis: {score_basis}")
    prefix = "RAW_" if score_basis == "raw" else ""

    def scored(name: str) -> Column:
        return F.col(f"{prefix}{name}")

    joined = clustered.join(labels, on="CLUSTER", how="inner")

    return joined.select(
        F.col("CUSTOMER_ID"),
        F.col("SEGMENT_NAME"),
        F.col("CLUSTER").cast("smallint").alias("SEGMENT_ID"),
        F.col("SUBSEGMENT_ID"),
        _round2(
            scored("LOG_BALANCE")
            * scored("TENURE_MONTHS")
            * scored("PRODUCT_BREADTH")
            * F.lit(10.0)
        ).alias("LIFETIME_VALUE_SCORE"),
        _round2(scored("ACCT_RATIO") * F.lit(100.0)).alias("ENGAGEMENT_SCORE"),
        F.col("DIGITAL_ADOPTION_SCORE").cast("double").alias("DIGITAL_ADOPTION_SCORE"),
        _round2(F.col("RAW_PRODUCT_BREADTH") * F.lit(100.0)).alias("PRODUCT_BREADTH_INDEX"),
        F.col("TENURE_GROUP"),
        F.col("AGE_GROUP"),
        F.col("BALANCE_TIER"),
        F.lit("").alias("CHANNEL_PREFERENCE"),
        F.when(
            (F.col("RAW_PRODUCT_BREADTH") < F.lit(0.50))
            & (scored("ACCT_RATIO") >= F.lit(0.75)),
            F.lit("Y"),
        )
        .otherwise(F.lit("N"))
        .alias("CROSS_SELL_FLAG"),
        F.when(
            (F.col("BALANCE_TIER") == F.lit("MODERATE"))
            & (F.col("TENURE_GROUP") != F.lit("NEW (<1yr)")),
            F.lit("Y"),
        )
        .otherwise(F.lit("N"))
        .alias("UPSELL_FLAG"),
        F.when(
            (scored("ACCT_RATIO") < F.lit(0.50)) & (F.col("RAW_TENURE_MONTHS") >= F.lit(60)),
            F.lit("Y"),
        )
        .otherwise(F.lit("N"))
        .alias("RETENTION_RISK_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.current_date().alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    ).select(*OUTPUT_COLUMNS)


def validate(df: DataFrame, min_rows: int = MIN_OUTPUT_ROWS) -> dict[str, int]:
    """STEP 7 - %validate_table equivalent: row count, key uniqueness, nulls."""
    not_null_columns = ["CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"]
    metrics = df.agg(
        F.count(F.lit(1)).alias("ROW_COUNT"),
        F.countDistinct("CUSTOMER_ID").alias("DISTINCT_CUSTOMERS"),
        *[
            F.sum(F.col(c).isNull().cast("long")).alias(f"NULL_{c}")
            for c in not_null_columns
        ],
    ).collect()[0].asDict()

    if metrics["ROW_COUNT"] < min_rows:
        raise ValidationError(
            f"CUSTOMER_SEGMENTS has {metrics['ROW_COUNT']} rows (minimum {min_rows})"
        )
    if metrics["ROW_COUNT"] != metrics["DISTINCT_CUSTOMERS"]:
        raise ValidationError("CUSTOMER_ID is not unique in CUSTOMER_SEGMENTS")
    for column in not_null_columns:
        if metrics[f"NULL_{column}"]:
            raise ValidationError(
                f"{column} has {metrics[f'NULL_{column}']} null values"
            )

    LOGGER.info("Validation passed: %s", metrics)
    return metrics


def write_product(df: DataFrame, config: SnowflakeConfig, table: str) -> None:
    """STEP 8 - truncate-and-load replacement for PROC APPEND."""
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**config.write_options())
        .option("dbtable", table)
        .option("truncate_table", "on")
        .option("column_mapping", "name")
        .mode("overwrite")
        .save()
    )


def run(
    spark: SparkSession,
    config: SnowflakeConfig,
    source_table: str,
    target_table: str,
    seed: int = DEFAULT_SEED,
    min_rows: int = MIN_OUTPUT_ROWS,
    score_basis: str = "raw",
    dry_run: bool = False,
) -> DataFrame:
    staging = read_staging(spark, config, source_table)
    segments = segment(staging, seed=seed, score_basis=score_basis)
    validate(segments, min_rows=min_rows)
    if dry_run:
        LOGGER.info("Dry run - skipping write to %s", target_table)
    else:
        write_product(segments, config, target_table)
    return segments


def segment(
    staging: DataFrame, seed: int = DEFAULT_SEED, score_basis: str = "raw"
) -> DataFrame:
    """Full in-memory transformation chain (STEP 2 - STEP 6)."""
    features = engineer_features(staging)
    standardised = standardise(features).cache()
    clustered = cluster(standardised, seed=seed).cache()
    labels = label_segments(clustered)
    return build_output(clustered, labels, score_basis=score_basis)


def _resolve_secret(key_vault: str | None, secret_name: str | None) -> str | None:
    """Fetch a secret from Azure Key Vault via mssparkutils when on Synapse."""
    if not key_vault or not secret_name:
        return None
    from notebookutils import mssparkutils  # noqa: PLC0415  (Synapse-only import)

    return mssparkutils.credentials.getSecret(key_vault, secret_name)


def parse_args(argv: list | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Customer segmentation (MBA-2207)")
    parser.add_argument("--sf-url", required=True, help="Snowflake account URL")
    parser.add_argument("--sf-user", required=True)
    parser.add_argument("--sf-role", default="TRANSFORMER")
    parser.add_argument("--sf-warehouse", default="WH_SPARK")
    parser.add_argument("--env", default="DEV", choices=["DEV", "UAT", "PROD"])
    parser.add_argument("--staging-database", default=None)
    parser.add_argument("--staging-schema", default="STAGING")
    parser.add_argument("--product-database", default=None)
    parser.add_argument("--product-schema", default="ANALYTICS")
    parser.add_argument("--source-table", default="STG_CUSTOMER_360")
    parser.add_argument("--target-table", default="CUSTOMER_SEGMENTS")
    parser.add_argument("--key-vault", default=None, help="Azure Key Vault linked service")
    parser.add_argument("--key-vault-secret", default=None, help="Secret holding the private key")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--min-rows", type=int, default=MIN_OUTPUT_ROWS)
    parser.add_argument("--score-basis", default="raw", choices=["raw", "standardised"])
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    config = SnowflakeConfig(
        url=args.sf_url,
        user=args.sf_user,
        role=args.sf_role,
        warehouse=args.sf_warehouse,
        staging_database=args.staging_database or f"ETL_STAGING_{args.env}",
        staging_schema=args.staging_schema,
        product_database=args.product_database or f"DATA_PRODUCTS_{args.env}",
        product_schema=args.product_schema,
        private_key=_resolve_secret(args.key_vault, args.key_vault_secret),
    )

    spark = SparkSession.builder.appName("mba-2207-customer-segments").getOrCreate()
    try:
        run(
            spark,
            config,
            source_table=args.source_table,
            target_table=args.target_table,
            seed=args.seed,
            min_rows=args.min_rows,
            score_basis=args.score_basis,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
