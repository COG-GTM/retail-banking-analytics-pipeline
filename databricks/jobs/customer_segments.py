"""Ticket 7 - Customer segmentation.

PySpark / Spark MLlib port of ``sas/01_sas_customer_segments.sas``.

Inputs  : etl_staging.stg_customer_360
Output  : data_products.customer_segments  (Delta)

    * PROC STDIZE method=std  -> ml.feature.StandardScaler (withMean, withStd)
    * PROC FASTCLUS k=5       -> ml.clustering.KMeans(k=5)
    * segment labels assigned by cluster-average balance (descending), exactly as
      the SAS CLUSTER_PROFILES / SEGMENT_LABELS step.

``segment_id`` is the raw (0-based) cluster index - an arbitrary identifier, as in
the reference outputs. SEGMENT_NAME carries the business meaning.

NOTE (business logic preserved): the SAS scores mix STANDARDISED cluster inputs
(``c.LOG_BALANCE``, ``c.TENURE_MONTHS``, ``c.PRODUCT_BREADTH``, ``c.ACCT_RATIO``)
with RAW engineered features (``f.PRODUCT_BREADTH``, ``f.TENURE_MONTHS`` ...). The
standardised scalars are read back from the scaler output so both sets stay
consistent with the values fed to KMeans.
"""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.clustering import KMeans

MODEL_VERSION = "SEG_V3.2"

CLUSTER_FEATURES = [
    "log_balance", "tenure_months", "credit_utilization_pct",
    "product_breadth", "acct_ratio", "age",
]

SEGMENT_LABELS = [
    "PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL",
    "CREDIT_DEPENDENT", "VALUE_BASIC",
]

OUTPUT_COLUMNS = [
    "customer_id", "segment_name", "segment_id", "subsegment_id",
    "lifetime_value_score", "engagement_score", "digital_adoption_score",
    "product_breadth_index", "tenure_group", "age_group", "balance_tier",
    "channel_preference", "cross_sell_flag", "upsell_flag", "retention_risk_flag",
    "model_version", "effective_date", "load_ts",
]


def engineer_features(stg_customer_360: DataFrame) -> DataFrame:
    active = stg_customer_360.where(F.col("customer_status") == "A")

    product_breadth = (
        (F.col("has_checking") == "Y").cast("double")
        + (F.col("has_savings") == "Y").cast("double")
        + (F.col("has_credit") == "Y").cast("double")
        + (F.col("has_loan") == "Y").cast("double")
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

    return active.select(
        F.col("customer_id"),
        F.col("age").cast("double").alias("age"),
        F.col("tenure_months").cast("double").alias("tenure_months"),
        F.col("credit_utilization_pct").cast("double").alias("credit_utilization_pct"),
        product_breadth.alias("product_breadth"),
        tenure_group.alias("tenure_group"),
        age_group.alias("age_group"),
        balance_tier.alias("balance_tier"),
        F.lit(0).cast("decimal(5,2)").alias("digital_adoption_score"),
        F.log(F.greatest(F.col("total_balance").cast("double"), F.lit(1.0))).alias("log_balance"),
        (
            F.col("num_active_accounts").cast("double")
            / F.greatest(F.col("num_accounts").cast("double"), F.lit(1.0))
        ).alias("acct_ratio"),
    )


def build_customer_segments(
    stg_customer_360: DataFrame,
    run_date: date,
    load_ts: datetime,
    seed: int = 42,
) -> DataFrame:
    features = engineer_features(stg_customer_360)

    assembler = VectorAssembler(inputCols=CLUSTER_FEATURES, outputCol="_raw_vec")
    scaler = StandardScaler(
        inputCol="_raw_vec", outputCol="_scaled_vec", withMean=True, withStd=True
    )
    assembled = assembler.transform(features)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    kmeans = KMeans(featuresCol="_scaled_vec", predictionCol="_cluster", k=5, maxIter=50, seed=seed)
    clustered = kmeans.fit(scaled).transform(scaled)

    std_arr = vector_to_array(F.col("_scaled_vec"))
    clustered = clustered.select(
        "customer_id",
        "tenure_group", "age_group", "balance_tier", "digital_adoption_score",
        "product_breadth", "tenure_months",
        F.col("_cluster").alias("cluster"),
        std_arr[0].alias("log_balance_std"),
        std_arr[1].alias("tenure_months_std"),
        std_arr[3].alias("product_breadth_std"),
        std_arr[4].alias("acct_ratio_std"),
    )

    # Cluster labelling by descending average (standardised) balance.
    profiles = clustered.groupBy("cluster").agg(F.avg("log_balance_std").alias("avg_balance"))
    rank_w = Window.orderBy(F.col("avg_balance").desc())
    labels = profiles.withColumn("_rank", F.row_number().over(rank_w))
    label_expr = F.lit(SEGMENT_LABELS[-1])
    for idx, name in enumerate(SEGMENT_LABELS, start=1):
        label_expr = F.when(F.col("_rank") == idx, name).otherwise(label_expr)
    labels = labels.select("cluster", label_expr.alias("segment_name"))

    lifetime_value = F.round(
        F.col("log_balance_std") * F.col("tenure_months_std") * F.col("product_breadth_std") * 10,
        2,
    )
    engagement = F.round(F.col("acct_ratio_std") * 100, 2)
    breadth_index = F.round(F.col("product_breadth") * 100, 2)

    cross_sell = F.when(
        (F.col("product_breadth") < 0.50) & (F.col("acct_ratio_std") >= 0.75), "Y"
    ).otherwise("N")
    upsell = F.when(
        (F.col("balance_tier") == "MODERATE") & (~F.col("tenure_group").isin("NEW (<1yr)")), "Y"
    ).otherwise("N")
    retention_risk = F.when(
        (F.col("acct_ratio_std") < 0.50) & (F.col("tenure_months") >= 60), "Y"
    ).otherwise("N")

    return (
        clustered.join(labels, "cluster", "inner")
        .select(
            F.col("customer_id"),
            F.col("segment_name"),
            F.col("cluster").cast("smallint").alias("segment_id"),
            F.lit(0).cast("smallint").alias("subsegment_id"),
            lifetime_value.cast("decimal(10,2)").alias("lifetime_value_score"),
            engagement.cast("decimal(5,2)").alias("engagement_score"),
            F.col("digital_adoption_score"),
            breadth_index.cast("decimal(5,2)").alias("product_breadth_index"),
            F.col("tenure_group"),
            F.col("age_group"),
            F.col("balance_tier"),
            F.lit("").alias("channel_preference"),
            cross_sell.alias("cross_sell_flag"),
            upsell.alias("upsell_flag"),
            retention_risk.alias("retention_risk_flag"),
            F.lit(MODEL_VERSION).alias("model_version"),
            F.lit(run_date).cast("date").alias("effective_date"),
            F.lit(load_ts).cast("timestamp").alias("load_ts"),
        )
    )
