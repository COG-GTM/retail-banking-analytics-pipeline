"""PySpark translation of sas/01_sas_customer_segments.sas.

Feature-engineers the customer-360 staging table, clusters active customers
with k-means (pyspark.ml replaces PROC STDIZE + PROC FASTCLUS), labels the
clusters by average balance, and emits the CUSTOMER_SEGMENTS data product.

Note: cluster assignments (SEGMENT_ID / SEGMENT_NAME) are model outputs and
are not bit-for-bit reproducible against a SAS FASTCLUS run; all other
columns are deterministic transformations validated against the golden data.
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_RUN_DATE
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MODEL_VERSION = "SEG_V3.2"

# Numeric features used for clustering (mirrors the PROC FASTCLUS VAR list)
CLUSTER_FEATURES = [
    "log_balance",
    "tenure_months",
    "credit_utilization_pct",
    "product_breadth",
    "acct_ratio",
    "age",
]


def build_customer_segments(
    spark: SparkSession,
    stg_customer_360: DataFrame,
    run_date: dt.date = DEFAULT_RUN_DATE,
    k: int = 5,
    seed: int = 42,
) -> DataFrame:
    """Return the CUSTOMER_SEGMENTS data product for active customers."""
    # STEP 1-2: filter to active customers and engineer features
    yn = lambda c: F.when(F.col(c) == "Y", 1.0).otherwise(0.0)
    feats = (
        stg_customer_360.filter(F.col("customer_status") == "A")
        .withColumn(
            # Proportion of the four product types held (SAS mean() of booleans)
            "product_breadth",
            (yn("has_checking") + yn("has_savings") + yn("has_credit") + yn("has_loan")) / 4,
        )
        .withColumn(
            "tenure_group",
            F.when(F.col("tenure_months") <= 12, "NEW (<1yr)")
            .when(F.col("tenure_months") <= 36, "DEVELOPING (1-3yr)")
            .when(F.col("tenure_months") <= 84, "ESTABLISHED (3-7yr)")
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
        # Digital adoption placeholder (enriched later by txn analytics)
        .withColumn("digital_adoption_score", F.lit(0.0))
        .withColumn("log_balance", F.log(F.greatest(F.col("total_balance"), F.lit(1.0))))
        .withColumn(
            "acct_ratio",
            F.col("num_active_accounts") / F.greatest(F.col("num_accounts"), F.lit(1)),
        )
    )

    # STEP 3-4: standardise features and run k-means
    # (VectorAssembler + StandardScaler + KMeans replace PROC STDIZE/FASTCLUS)
    assembled = VectorAssembler(
        inputCols=CLUSTER_FEATURES, outputCol="raw_features", handleInvalid="keep"
    ).transform(feats)
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withMean=True, withStd=True
    ).fit(assembled)
    scaled = scaler.transform(assembled)
    model = KMeans(k=k, maxIter=50, tol=0.001, seed=seed, featuresCol="features").fit(scaled)
    clustered = model.transform(scaled).withColumnRenamed("prediction", "cluster")

    # STEP 5: label clusters by descending average balance
    profiles = (
        clustered.groupBy("cluster")
        .agg(F.avg("log_balance").alias("avg_balance"))
        .orderBy(F.desc("avg_balance"))
        .collect()
    )
    names = [
        "PREMIUM_WEALTH",
        "ENGAGED_MAINSTREAM",
        "GROWING_DIGITAL",
        "CREDIT_DEPENDENT",
        "VALUE_BASIC",
    ]
    label_map = {row["cluster"]: names[min(i, len(names) - 1)] for i, row in enumerate(profiles)}
    label_expr = F.create_map(
        *[F.lit(x) for pair in label_map.items() for x in pair]
    )[F.col("cluster")]

    # STEP 6: merge labels back, compute scores, set action flags
    return clustered.select(
        "customer_id",
        label_expr.alias("segment_name"),
        F.col("cluster").alias("segment_id"),
        F.lit(0).alias("subsegment_id"),
        # Lifetime value heuristic: balance x tenure x breadth
        F.round(
            F.col("log_balance") * F.col("tenure_months") * F.col("product_breadth") * 10, 2
        ).alias("lifetime_value_score"),
        F.round(F.col("acct_ratio") * 100, 2).alias("engagement_score"),
        "digital_adoption_score",
        F.round(F.col("product_breadth") * 100, 2).alias("product_breadth_index"),
        "tenure_group",
        "age_group",
        "balance_tier",
        # Channel preference placeholder (enriched later by txn analytics)
        F.lit("").alias("channel_preference"),
        # Cross-sell: low product breadth but good engagement
        F.when((F.col("product_breadth") < 0.50) & (F.col("acct_ratio") >= 0.75), "Y")
        .otherwise("N")
        .alias("cross_sell_flag"),
        # Upsell: moderate balance with room to grow
        F.when(
            (F.col("balance_tier") == "MODERATE") & (F.col("tenure_group") != "NEW (<1yr)"), "Y"
        )
        .otherwise("N")
        .alias("upsell_flag"),
        # Retention risk: low engagement and long tenure
        F.when((F.col("acct_ratio") < 0.50) & (F.col("tenure_months") >= 60), "Y")
        .otherwise("N")
        .alias("retention_risk_flag"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(run_date).alias("effective_date"),
        F.current_timestamp().alias("load_ts"),
    )
