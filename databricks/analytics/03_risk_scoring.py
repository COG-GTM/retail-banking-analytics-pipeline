# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Risk Scoring (PySpark + scikit-learn)
# MAGIC
# MAGIC Port of `sas/03_sas_risk_scoring.sas`. Mirrors the logic in
# MAGIC `local/duckdb/run_demo.py::_phase3c_risk_scoring`.
# MAGIC
# MAGIC | SAS                                  | Databricks                               |
# MAGIC |--------------------------------------|------------------------------------------|
# MAGIC | `PROC LOGISTIC selection=stepwise`   | `sklearn LogisticRegression` (lbfgs)     |
# MAGIC | weighted composite (30/25/15/20/10)  | preserved exactly                        |
# MAGIC | tier cuts 20/40/60/80                 | `LOW/MODERATE/ELEVATED/HIGH/CRITICAL`    |
# MAGIC | array top-2 driver logic             | `argsort` of the four component scores   |
# MAGIC | `PROC APPEND ... FORCE` / `DELETE`   | `df.write.mode("overwrite").saveAsTable` |
# MAGIC
# MAGIC The target (`payment_late_cnt > 2`) can collapse to a single class on small
# MAGIC samples; the reference's heuristic fallback is preserved.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

from datetime import date, datetime

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

ensure_audit_table()
guard("sas", "03_risk_scoring")
log_step(step="03_risk_scoring", status="START", msg=f"Model version {MODEL_VERSION_RISK}")

# COMMAND ----------

# Step 1: extract risk factors joined with baseline customer attributes
df = (
    spark.sql(  # noqa: F821
        f"""
        SELECT r.*, c.tenure_months, c.num_active_accounts, c.total_balance, c.customer_status
        FROM {stg("stg_risk_factors")} r
        JOIN {stg("stg_customer_360")} c ON r.customer_id = c.customer_id
        WHERE c.customer_status = 'A'
        """
    )
    .drop("load_ts")
    .toPandas()
)
log_step(step="03_risk_scoring", status="SUCCESS", msg="Extracted risk factors", rowcount=len(df))

# COMMAND ----------

# Step 2: feature preparation (mirrors SAS STEP 2)
df["external_credit_score"] = df["external_credit_score"].replace(0, 680).fillna(680)
df["bureau_score_norm"] = (df["external_credit_score"] - 300) / (850 - 300) * 100
df["balance_trend_ratio"] = np.where(
    df["avg_daily_balance_90d"] > 0,
    df["avg_daily_balance_30d"] / df["avg_daily_balance_90d"], 1.0)
df["velocity_ratio"] = np.where(
    df["debit_velocity_30d"] > 0,
    (df["debit_velocity_7d"] * (30 / 7)) / df["debit_velocity_30d"], 1.0)
df["default_flag"] = (df["payment_late_cnt"] > 2).astype(int)

# COMMAND ----------

# Step 3: logistic regression (mirrors PROC LOGISTIC)
feat_cols = ["bureau_score_norm", "credit_util_ratio", "payment_ontime_pct",
             "balance_volatility", "velocity_ratio", "account_overdraft_cnt",
             "large_withdrawal_cnt", "high_risk_merchant_cnt", "tenure_months"]
X = df[feat_cols].fillna(0).astype(float).values
y = df["default_flag"].values
X_scaled = StandardScaler().fit_transform(X)
try:
    lr = LogisticRegression(max_iter=200, solver="lbfgs", random_state=42)
    lr.fit(X_scaled, y)
    df["prob_default"] = lr.predict_proba(X_scaled)[:, 1]
except Exception:
    # Only one class present -> heuristic fallback (matches reference)
    df["prob_default"] = df["default_flag"] * 0.8 + 0.05

# COMMAND ----------

# Step 4: composite score, tier, drivers, flags (mirrors SAS STEP 4)
df["credit_risk_component"] = np.clip(100 - df["bureau_score_norm"], 0, 100)
df["behaviour_risk_component"] = np.clip(100 - df["payment_ontime_pct"], 0, 100)
df["velocity_risk_component"] = np.clip((df["velocity_ratio"] - 1) * 50, 0, 100)
df["bureau_score_component"] = np.clip(df["bureau_score_norm"], 0, 100)
df["payment_history_component"] = np.clip(df["payment_ontime_pct"], 0, 100)

df["composite_risk_score"] = (
    df["credit_risk_component"]    * 0.30 +
    df["behaviour_risk_component"] * 0.25 +
    df["velocity_risk_component"]  * 0.15 +
    (100 - df["bureau_score_component"])    * 0.20 +
    (100 - df["payment_history_component"]) * 0.10
).round(2)

df["probability_of_default"] = df["prob_default"].round(6)

import pandas as pd  # noqa: E402

df["risk_tier"] = pd.cut(
    df["composite_risk_score"], bins=[-0.01, 20, 40, 60, 80, 100.01],
    labels=["LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"]).astype(str)

# Primary & secondary risk drivers (mirrors SAS array logic)
driver_cols = {
    "credit_risk_component": "CREDIT_UTILIZATION",
    "behaviour_risk_component": "PAYMENT_BEHAVIOUR",
    "velocity_risk_component": "TRANSACTION_VELOCITY",
}
driver_df = df[list(driver_cols.keys())].copy()
driver_df["inv_bureau"] = 100 - df["bureau_score_component"]
driver_labels = list(driver_cols.values()) + ["BUREAU_SCORE"]


def _top2(row):
    idx = np.argsort(row.values)[::-1]
    return driver_labels[idx[0]], driver_labels[idx[1]]


drivers = driver_df.apply(_top2, axis=1, result_type="expand")
df["primary_risk_driver"] = drivers[0]
df["secondary_risk_driver"] = drivers[1]

df["score_delta_30d"] = 0.0
df["watch_list_flag"] = np.where(
    (df["risk_tier"] == "CRITICAL") & (df["probability_of_default"] > 0.5), "Y", "N")
df["review_required_flag"] = np.where(
    (df["composite_risk_score"] >= 60) & (df["velocity_ratio"] > 2.0), "Y", "N")
df["model_version"] = MODEL_VERSION_RISK
df["effective_date"] = date.today()
df["load_ts"] = datetime.now()

# COMMAND ----------

out_cols = ["customer_id", "composite_risk_score", "risk_tier", "probability_of_default",
            "credit_risk_component", "behaviour_risk_component", "velocity_risk_component",
            "bureau_score_component", "payment_history_component",
            "primary_risk_driver", "secondary_risk_driver", "score_delta_30d",
            "watch_list_flag", "review_required_flag", "model_version",
            "effective_date", "load_ts"]
out_pdf = df[out_cols].copy()

spark.createDataFrame(out_pdf).write.mode("overwrite").option(  # noqa: F821
    "overwriteSchema", "true").saveAsTable(dp("customer_risk_scores"))

rc = validate_table(
    table=dp("customer_risk_scores"),
    key_cols=["customer_id"],
    not_null=["customer_id", "composite_risk_score", "risk_tier"],
    min_rows=1,
)
if rc != 0:
    log_step(step="03_risk_scoring", status="ERROR", msg="Validation failed")
    raise Exception("Validation failed for customer_risk_scores")

# Risk tier distribution for monitoring (mirrors PROC FREQ)
display(spark.table(dp("customer_risk_scores")).groupBy("risk_tier").count().orderBy("risk_tier"))  # noqa: F821

n = spark.table(dp("customer_risk_scores")).count()  # noqa: F821
log_step(step="03_risk_scoring", status="SUCCESS", msg="Pipeline complete", rowcount=n)
dbutils.notebook.exit(f"customer_risk_scores: {n} rows")  # noqa: F821
