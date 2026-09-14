import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from ..audit import assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..ml import log_sklearn_model, mlflow_run
from ..tables import write_overwrite

MODEL_VERSION = "SEG_V3.2"
_LABELS = [
    "PREMIUM_WEALTH",
    "ENGAGED_MAINSTREAM",
    "GROWING_DIGITAL",
    "CREDIT_DEPENDENT",
    "VALUE_BASIC",
]
_FEATURES = [
    "log_balance",
    "tenure_months",
    "credit_utilization_pct",
    "product_breadth",
    "acct_ratio",
    "age",
]


def _features(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    source = spark.table(cfg.fqn(cfg.silver_schema, "stg_customer_360")).where(
        F.col("customer_status") == "A"
    )
    product_breadth = (
        F.when(F.col("has_checking") == "Y", 1).otherwise(0)
        + F.when(F.col("has_savings") == "Y", 1).otherwise(0)
        + F.when(F.col("has_credit") == "Y", 1).otherwise(0)
        + F.when(F.col("has_loan") == "Y", 1).otherwise(0)
    ) / F.lit(4.0)
    return source.select(
        "customer_id",
        "age",
        "tenure_months",
        "credit_utilization_pct",
        "total_balance",
        "num_accounts",
        "num_active_accounts",
        product_breadth.alias("product_breadth"),
        F.log(F.greatest(F.col("total_balance"), F.lit(1))).alias("log_balance"),
        (F.col("num_active_accounts") / F.greatest(F.col("num_accounts"), F.lit(1))).alias(
            "acct_ratio"
        ),
    )


def _cluster_assignments(features: DataFrame):
    selected = features.select("customer_id", "total_balance", *_FEATURES).toPandas()
    selected["total_balance"] = selected["total_balance"].astype(float)
    numeric = selected[_FEATURES].astype(float)
    scaler = StandardScaler()
    standardized = scaler.fit_transform(numeric)
    model = KMeans(
        n_clusters=5,
        max_iter=50,
        tol=0.001,
        n_init=10,
        random_state=42,
    )
    selected["segment_id"] = model.fit_predict(standardized).astype(int)
    assignments = selected[["customer_id", "segment_id"]].astype({"customer_id": "int64"})
    return assignments, model


def _build_result(spark: SparkSession, cfg: RunConfig):
    features = _features(spark, cfg)
    assignments, model = _cluster_assignments(features)
    assignment_schema = StructType(
        [
            StructField("customer_id", LongType(), False),
            StructField("segment_id", LongType(), False),
        ]
    )
    assignment_df = spark.createDataFrame(
        assignments.itertuples(index=False, name=None), assignment_schema
    )
    source = features.join(assignment_df, "customer_id")
    result = (
        source.withColumn(
            "tenure_group",
            F.when(F.col("tenure_months") <= 12, "NEW (<1yr)")
            .when(F.col("tenure_months") <= 36, "DEVELOPING (1-3yr)")
            .when(F.col("tenure_months") < 84, "ESTABLISHED (3-7yr)")
            .otherwise("LOYAL (7yr+)"),
        )
        .withColumn(
            "age_group",
            F.when(F.col("age") <= 25, "GEN_Z")
            .when(F.col("age") <= 41, "MILLENNIAL")
            .when(F.col("age") <= 57, "GEN_X")
            .when(F.col("age") <= 76, "BOOMER")
            .otherwise("SILENT"),
        )
        .withColumn(
            "balance_tier",
            F.when(F.col("total_balance") < 1000, "LOW")
            .when(F.col("total_balance") < 10000, "MODERATE")
            .when(F.col("total_balance") < 100000, "AFFLUENT")
            .otherwise("HIGH_NET_WORTH"),
        )
        .select(
            "customer_id",
            F.lit(0).cast("smallint").alias("subsegment_id"),
            F.round(
                F.col("log_balance") * F.col("tenure_months") * F.col("product_breadth") * 10,
                2,
            )
            .cast("decimal(10,2)")
            .alias("lifetime_value_score"),
            F.round(F.col("acct_ratio") * 100, 2).cast("decimal(5,2)").alias("engagement_score"),
            F.lit(0).cast("decimal(5,2)").alias("digital_adoption_score"),
            F.round(F.col("product_breadth") * 100, 2)
            .cast("decimal(5,2)")
            .alias("product_breadth_index"),
            "tenure_group",
            "age_group",
            "balance_tier",
            F.lit(None).cast("string").alias("channel_preference"),
            F.when((F.col("product_breadth") < 0.5) & (F.col("acct_ratio") >= 0.75), "Y")
            .otherwise("N")
            .alias("cross_sell_flag"),
            F.when(
                (F.col("balance_tier") == "MODERATE") & (F.col("tenure_group") != "NEW (<1yr)"),
                "Y",
            )
            .otherwise("N")
            .alias("upsell_flag"),
            F.when((F.col("acct_ratio") < 0.5) & (F.col("tenure_months") >= 60), "Y")
            .otherwise("N")
            .alias("retention_risk_flag"),
            F.lit(MODEL_VERSION).alias("model_version"),
            F.lit(cfg.run_date).cast("date").alias("effective_date"),
            F.current_timestamp().alias("load_ts"),
            F.col("segment_id").cast("smallint").alias("segment_id"),
        )
    )
    profile = (
        features.join(assignment_df, "customer_id")
        .groupBy("segment_id")
        .agg(F.avg("total_balance").alias("avg_balance"))
        .orderBy(F.col("avg_balance").desc())
    )
    names = [row.segment_id for row in profile.collect()]
    label_map = {segment_id: label for segment_id, label in zip(names, _LABELS)}
    mapping = spark.createDataFrame(
        [(int(key), value) for key, value in label_map.items()],
        ["segment_id", "segment_name"],
    )
    result = result.join(mapping, "segment_id").select(
        "customer_id",
        "segment_name",
        "segment_id",
        "subsegment_id",
        "lifetime_value_score",
        "engagement_score",
        "digital_adoption_score",
        "product_breadth_index",
        "tenure_group",
        "age_group",
        "balance_tier",
        "channel_preference",
        "cross_sell_flag",
        "upsell_flag",
        "retention_risk_flag",
        "model_version",
        "effective_date",
        "load_ts",
    )
    return result, model


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    return _build_result(spark, cfg)[0]


def run(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    fqn = cfg.fqn(cfg.gold_schema, "customer_segments")
    with step(spark, cfg, "01_customer_segments", "FULL_LOAD") as state:
        result, model = _build_result(spark, cfg)
        state["row_count"] = assert_rows(result, "customer_segments")
        with mlflow_run(cfg, "customer_segments") as active_run:
            if active_run is not None:
                log_sklearn_model(
                    model,
                    "customer_segments_kmeans",
                    {
                        "k": 5,
                        "max_iter": 50,
                        "tol": 0.001,
                        "model_version": MODEL_VERSION,
                    },
                    {
                        "inertia": model.inertia_,
                        **{
                            f"cluster_size_{index}": int(size)
                            for index, size in enumerate(np.bincount(model.labels_, minlength=5))
                        },
                    },
                )
        write_overwrite(result, fqn)
    validate_table(
        spark, fqn, ["customer_id"], ["customer_id", "segment_name", "segment_id"], cfg.dq_min_rows
    )
    return result
