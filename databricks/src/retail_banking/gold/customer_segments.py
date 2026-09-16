from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler

SEGMENT_LABELS = [
    "PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL",
    "CREDIT_DEPENDENT", "VALUE_BASIC",
]
FEATURE_COLS = ["log_balance", "tenure_months", "credit_utilization_pct",
                "product_breadth", "acct_ratio", "age"]
OUT_COLS = ["customer_id", "segment_name", "segment_id", "subsegment_id",
            "lifetime_value_score", "engagement_score", "digital_adoption_score",
            "product_breadth_index", "tenure_group", "age_group",
            "balance_tier", "channel_preference", "cross_sell_flag",
            "upsell_flag", "retention_risk_flag", "model_version",
            "effective_date", "load_ts"]


def build_customer_segments(stg_customer_360, run_date: date, seed: int = 42):
    """Port of sas/01_sas_customer_segments.sas (phase3a)."""
    df = stg_customer_360.filter(F.col("customer_status") == "A").select(
        "customer_id", "age", "tenure_months", "customer_status",
        "segment_code", "state_code", "num_accounts", "num_active_accounts",
        "has_checking", "has_savings", "has_credit", "has_loan",
        "total_balance", "total_credit_limit", "credit_utilization_pct")

    df = df.withColumn(
        "product_breadth",
        ((F.when(F.col("has_checking") == "Y", 1).otherwise(0)
          + F.when(F.col("has_savings") == "Y", 1).otherwise(0)
          + F.when(F.col("has_credit") == "Y", 1).otherwise(0)
          + F.when(F.col("has_loan") == "Y", 1).otherwise(0)) / 4.0))
    # pd.cut right-inclusive bins, exactly as the reference
    df = df.withColumn("tenure_group", F.when(F.col("tenure_months") <= 12, "NEW (<1yr)")
        .when(F.col("tenure_months") <= 36, "DEVELOPING (1-3yr)")
        .when(F.col("tenure_months") <= 84, "ESTABLISHED (3-7yr)")
        .otherwise("LOYAL (7yr+)"))
    df = df.withColumn("age_group", F.when(F.col("age") <= 25, "GEN_Z")
        .when(F.col("age") <= 41, "MILLENNIAL")
        .when(F.col("age") <= 57, "GEN_X")
        .when(F.col("age") <= 76, "BOOMER")
        .otherwise("SILENT"))
    df = df.withColumn("balance_tier", F.when(F.col("total_balance") <= 1000, "LOW")
        .when(F.col("total_balance") <= 10000, "MODERATE")
        .when(F.col("total_balance") <= 100000, "AFFLUENT")
        .otherwise("HIGH_NET_WORTH"))
    df = df.withColumn("log_balance",
                       F.log(F.greatest(
                           F.coalesce(F.col("total_balance"), F.lit(0.0)),
                           F.lit(1.0))))
    df = df.withColumn("acct_ratio",
                       F.coalesce(F.col("num_active_accounts"), F.lit(0.0))
                       / F.greatest(
                           F.coalesce(F.col("num_accounts"), F.lit(1.0)),
                           F.lit(1.0)))

    for c in FEATURE_COLS:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)).cast("double"))

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="_feat_raw",
                                handleInvalid="keep")
    scaler = StandardScaler(inputCol="_feat_raw", outputCol="features",
                            withMean=True, withStd=True)
    km = KMeans(k=5, maxIter=50, tol=0.001, seed=seed,
                featuresCol="features", predictionCol="cluster")

    assembled = assembler.transform(df)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)
    km_model = km.fit(scaled)
    df = km_model.transform(scaled)

    # Label clusters by mean log_balance descending (SAS STEP 5)
    cluster_avg = (df.groupBy("cluster")
        .agg(F.avg("log_balance").alias("m"))
        .orderBy(F.col("m").desc())
        .collect())
    label_map = {int(r["cluster"]): SEGMENT_LABELS[i]
                 for i, r in enumerate(cluster_avg)}
    label_expr = F.create_map(*[F.lit(x) for kv in label_map.items() for x in kv])

    df = df.withColumn("segment_name", label_expr[F.col("cluster")])
    df = df.withColumn("segment_id", F.col("cluster").cast("smallint"))
    df = df.withColumn("subsegment_id", F.lit(0).cast("smallint"))

    df = df.withColumn("lifetime_value_score",
        F.round(F.col("log_balance") * F.col("tenure_months")
                * F.col("product_breadth") * 10, 2).cast("decimal(10,2)"))
    df = df.withColumn("engagement_score",
        F.round(F.col("acct_ratio") * 100, 2).cast("decimal(5,2)"))
    df = df.withColumn("digital_adoption_score",
                       F.lit(0.0).cast("decimal(5,2)"))
    df = df.withColumn("product_breadth_index",
        F.round(F.col("product_breadth") * 100, 2).cast("decimal(5,2)"))
    df = df.withColumn("channel_preference", F.lit(""))
    df = df.withColumn("cross_sell_flag", F.when(
        (F.col("product_breadth") < 0.50) & (F.col("acct_ratio") >= 0.75),
        "Y").otherwise("N"))
    df = df.withColumn("upsell_flag", F.when(
        (F.col("balance_tier") == "MODERATE")
        & (F.col("tenure_group") != "NEW (<1yr)"), "Y").otherwise("N"))
    df = df.withColumn("retention_risk_flag", F.when(
        (F.col("acct_ratio") < 0.50) & (F.col("tenure_months") >= 60),
        "Y").otherwise("N"))
    df = df.withColumn("model_version", F.lit("SEG_V3.2"))
    df = df.withColumn("effective_date", F.lit(run_date))
    df = df.withColumn("load_ts", F.current_timestamp())

    return df.select(*OUT_COLS)
