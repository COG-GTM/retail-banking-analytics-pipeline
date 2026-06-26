# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Customer Segments (PySpark + scikit-learn)
# MAGIC
# MAGIC Port of `sas/01_sas_customer_segments.sas`. Mirrors the already-translated
# MAGIC logic in `local/duckdb/run_demo.py::_phase3a_customer_segments`.
# MAGIC
# MAGIC | SAS                              | Databricks                                   |
# MAGIC |---------------------------------|----------------------------------------------|
# MAGIC | `PROC STDIZE method=std`        | `sklearn StandardScaler`                     |
# MAGIC | `PROC FASTCLUS maxclusters=5`   | `sklearn KMeans(n_clusters=5)`               |
# MAGIC | cluster labelling by avg balance| label by `log_balance` mean desc             |
# MAGIC | `PROC APPEND ... FORCE` / `DELETE` | `df.write.mode("overwrite").saveAsTable()` |
# MAGIC
# MAGIC The customer-level dataset is small (one row per active customer), so the
# MAGIC clustering runs on the driver with scikit-learn to preserve identical
# MAGIC results to the reference implementation. Spark MLlib `StandardScaler` +
# MAGIC `KMeans` are a drop-in alternative if driver-side memory is a concern.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

from datetime import date, datetime

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

ensure_audit_table()
guard("sas", "01_customer_segments")
log_step(step="01_customer_segments", status="START", msg="Beginning customer segmentation")

# COMMAND ----------

# Step 1: pull active customers from staging
df = (
    spark.table(stg("stg_customer_360"))  # noqa: F821
    .filter("customer_status = 'A'")
    .select(
        "customer_id", "age", "tenure_months", "customer_status", "segment_code",
        "state_code", "num_accounts", "num_active_accounts",
        "has_checking", "has_savings", "has_credit", "has_loan",
        "total_balance", "total_credit_limit", "credit_utilization_pct",
    )
    .toPandas()
)
log_step(step="01_customer_segments", status="SUCCESS", msg="Extracted staging data", rowcount=len(df))

# COMMAND ----------

# Step 2: feature engineering (mirrors SAS STEP 2)
df["product_breadth"] = df[["has_checking", "has_savings", "has_credit", "has_loan"]].apply(
    lambda r: sum(1 for v in r if v == "Y") / 4.0, axis=1)
df["tenure_group"] = pd.cut(
    df["tenure_months"], bins=[-1, 12, 36, 84, 9999],
    labels=["NEW (<1yr)", "DEVELOPING (1-3yr)", "ESTABLISHED (3-7yr)", "LOYAL (7yr+)"])
df["age_group"] = pd.cut(
    df["age"], bins=[0, 25, 41, 57, 76, 120],
    labels=["GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER", "SILENT"])
df["balance_tier"] = pd.cut(
    df["total_balance"], bins=[-float("inf"), 1000, 10000, 100000, float("inf")],
    labels=["LOW", "MODERATE", "AFFLUENT", "HIGH_NET_WORTH"])
df["log_balance"] = np.log(np.maximum(df["total_balance"].fillna(0).astype(float), 1))
df["acct_ratio"] = (df["num_active_accounts"].fillna(0).astype(float)
                    / np.maximum(df["num_accounts"].fillna(1).astype(float), 1))

# COMMAND ----------

# Step 3 + 4: standardise + k-means (mirrors PROC STDIZE + PROC FASTCLUS)
feat_cols = ["log_balance", "tenure_months", "credit_utilization_pct",
             "product_breadth", "acct_ratio", "age"]
X = df[feat_cols].fillna(0).astype(float).values
X_scaled = StandardScaler().fit_transform(X)
km = KMeans(n_clusters=5, max_iter=50, tol=0.001, n_init=10, random_state=42)
df["cluster"] = km.fit_predict(X_scaled)

# Step 5: label clusters by average (log) balance descending
labels = ["PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL",
          "CREDIT_DEPENDENT", "VALUE_BASIC"]
cluster_avg = df.groupby("cluster")["log_balance"].mean().sort_values(ascending=False)
label_map = {c: labels[i] for i, c in enumerate(cluster_avg.index)}
df["segment_name"] = df["cluster"].map(label_map)
df["segment_id"] = df["cluster"]
df["subsegment_id"] = 0

# COMMAND ----------

# Step 6: scores & action flags (mirrors SAS STEP 6)
df["lifetime_value_score"] = (
    df["log_balance"] * df["tenure_months"] * df["product_breadth"] * 10).round(2)
df["engagement_score"] = (df["acct_ratio"] * 100).round(2)
df["digital_adoption_score"] = 0.0
df["product_breadth_index"] = (df["product_breadth"] * 100).round(2)
df["channel_preference"] = ""
df["cross_sell_flag"] = np.where(
    (df["product_breadth"] < 0.50) & (df["acct_ratio"] >= 0.75), "Y", "N")
df["upsell_flag"] = np.where(
    (df["balance_tier"] == "MODERATE") & (df["tenure_group"] != "NEW (<1yr)"), "Y", "N")
df["retention_risk_flag"] = np.where(
    (df["acct_ratio"] < 0.50) & (df["tenure_months"] >= 60), "Y", "N")
df["model_version"] = MODEL_VERSION_SEGMENTS
df["effective_date"] = date.today()
df["load_ts"] = datetime.now()

out_cols = ["customer_id", "segment_name", "segment_id", "subsegment_id",
            "lifetime_value_score", "engagement_score", "digital_adoption_score",
            "product_breadth_index", "tenure_group", "age_group", "balance_tier",
            "channel_preference", "cross_sell_flag", "upsell_flag",
            "retention_risk_flag", "model_version", "effective_date", "load_ts"]
out_pdf = df[out_cols].copy()
# Convert pandas categoricals to plain strings for Spark
for c in ["tenure_group", "age_group", "balance_tier"]:
    out_pdf[c] = out_pdf[c].astype(str)
out_pdf["segment_id"] = out_pdf["segment_id"].astype(int)

# COMMAND ----------

# Step 7 + 8: write to the data product table (overwrite = truncate-and-load)
spark.createDataFrame(out_pdf).write.mode("overwrite").option(  # noqa: F821
    "overwriteSchema", "true").saveAsTable(dp("customer_segments"))

rc = validate_table(
    table=dp("customer_segments"),
    key_cols=["customer_id"],
    not_null=["customer_id", "segment_name", "segment_id"],
    min_rows=1,
)
if rc != 0:
    log_step(step="01_customer_segments", status="ERROR", msg="Validation failed")
    raise Exception("Validation failed for customer_segments")

n = spark.table(dp("customer_segments")).count()  # noqa: F821
log_step(step="01_customer_segments", status="SUCCESS", msg="Pipeline complete", rowcount=n)
dbutils.notebook.exit(f"customer_segments: {n} rows")  # noqa: F821
