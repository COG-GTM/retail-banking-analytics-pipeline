# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Transaction Analytics (PySpark)
# MAGIC
# MAGIC Port of `sas/02_sas_txn_analytics.sas`. Mirrors the logic in
# MAGIC `local/duckdb/run_demo.py::_phase3b_txn_analytics`, implemented with native
# MAGIC Spark window functions / `percentile_approx` as requested.
# MAGIC
# MAGIC | SAS                              | Databricks                                   |
# MAGIC |---------------------------------|----------------------------------------------|
# MAGIC | account->customer `PROC SQL`    | DataFrame `groupBy().agg()`                   |
# MAGIC | `PROC RANK groups=100`          | `percent_rank()` window * 100                |
# MAGIC | `PROC MEANS` median / qrange    | `percentile_approx(..., array(0.25,0.5,0.75))`|
# MAGIC | IQR anomaly: `> median + 3*IQR` | same rule via computed median/IQR            |
# MAGIC | `PROC APPEND ... FORCE`         | `df.write.mode("overwrite").saveAsTable()`   |

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

from datetime import date, datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

ensure_audit_table()
guard("sas", "02_txn_analytics")
reporting_period = date.today().strftime("%Y-%m")
log_step(step="02_txn_analytics", status="START", msg=f"Period: {reporting_period}")

# COMMAND ----------

# Step 1: pull transaction summary from staging
txn_stg = spark.table(stg("stg_txn_summary"))  # noqa: F821
log_step(step="02_txn_analytics", status="SUCCESS",
         msg="Extracted stg_txn_summary", rowcount=txn_stg.count())

# COMMAND ----------

# Step 2: aggregate account-level -> customer-level (mirrors SAS STEP 2)
cust = txn_stg.groupBy("customer_id").agg(
    F.countDistinct("account_id").alias("total_accounts"),
    F.sum(F.when(F.col("days_since_last_txn") <= 30, 1).otherwise(0)).alias("active_accounts"),
    F.sum("txn_count_total").alias("total_transactions"),
    F.sum("amt_total_debit").alias("total_debit_amt"),
    F.sum("amt_total_credit").alias("total_credit_amt"),
    F.sum("amt_total_fees").alias("total_fees"),
    F.sum((F.col("txn_count_total") * (F.coalesce(F.col("pct_web"), F.lit(0))
           + F.coalesce(F.col("pct_mobile"), F.lit(0))) / 100.0)).alias("_dig_txns"),
    F.first("top_merchant_category").alias("top_spend_category"),
)

cust = (
    cust
    .withColumn("net_cash_flow", F.col("total_credit_amt") - F.col("total_debit_amt"))
    .withColumn("avg_transaction_size",
                F.when(F.col("total_transactions") > 0,
                       (F.col("total_debit_amt") + F.col("total_credit_amt"))
                       / F.col("total_transactions")).otherwise(F.lit(0.0)))
    .withColumn("digital_txn_pct",
                F.when(F.col("total_transactions") > 0,
                       F.col("_dig_txns") / F.col("total_transactions") * 100).otherwise(F.lit(0.0)))
    .drop("_dig_txns")
)

# COMMAND ----------

# Step 3: spend trend + revenue components (mirrors SAS STEP 3)
cust = (
    cust
    .withColumn("monthly_spend_trend",
                F.when(F.col("net_cash_flow") > F.col("avg_transaction_size") * 5, F.lit("UP"))
                 .when(F.col("net_cash_flow") < -F.col("avg_transaction_size") * 5, F.lit("DOWN"))
                 .otherwise(F.lit("STABLE")))
    .withColumn("fee_income", F.col("total_fees"))
    .withColumn("interest_income", F.round(F.col("total_debit_amt") * 0.02, 2))
    .withColumn("revenue_contribution", F.col("total_fees") + F.round(F.col("total_debit_amt") * 0.02, 2))
)

# COMMAND ----------

# Step 4: percentile ranking for spend (mirrors PROC RANK groups=100)
w = Window.orderBy("total_debit_amt")
cust = cust.withColumn("spend_percentile",
                       F.round(F.percent_rank().over(w) * 100, 2))

# COMMAND ----------

# Step 5: IQR anomaly detection (mirrors PROC MEANS median/qrange + data step)
stats = cust.select(
    F.percentile_approx("total_debit_amt", F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75))).alias("q")
).collect()[0]["q"]
q1, median, q3 = float(stats[0]), float(stats[1]), float(stats[2])
iqr = q3 - q1
anomaly_threshold = median + 3 * iqr
print(f"  median={median:,.2f}  IQR={iqr:,.2f}  anomaly_threshold={anomaly_threshold:,.2f}")

cust = cust.withColumn(
    "anomaly_flag",
    F.when((F.col("total_debit_amt") > F.lit(anomaly_threshold)) & (F.lit(iqr) > 0), F.lit("Y"))
     .otherwise(F.lit("N")))

# COMMAND ----------

# Metadata + final column selection matching the data product schema
final = (
    cust
    .withColumn("reporting_period", F.lit(reporting_period))
    .withColumn("model_version", F.lit(MODEL_VERSION_TXN))
    .withColumn("effective_date", F.lit(date.today()))
    .withColumn("load_ts", F.lit(datetime.now()))
    .select(
        "customer_id", "reporting_period",
        F.col("total_accounts").cast("smallint").alias("total_accounts"),
        F.col("active_accounts").cast("smallint").alias("active_accounts"),
        F.col("total_transactions").cast("int").alias("total_transactions"),
        "total_debit_amt", "total_credit_amt", "net_cash_flow", "avg_transaction_size",
        "monthly_spend_trend", "spend_percentile", "top_spend_category", "digital_txn_pct",
        "fee_income", "interest_income", "revenue_contribution", "anomaly_flag",
        "model_version", "effective_date", "load_ts",
    )
)

# COMMAND ----------

# Step 6 + 7: write, validate, log
final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dp("transaction_analytics"))

rc = validate_table(
    table=dp("transaction_analytics"),
    key_cols=["customer_id"],
    not_null=["customer_id", "reporting_period", "total_transactions"],
    min_rows=1,
)
if rc != 0:
    log_step(step="02_txn_analytics", status="ERROR", msg="Validation failed")
    raise Exception("Validation failed for transaction_analytics")

n = spark.table(dp("transaction_analytics")).count()  # noqa: F821
log_step(step="02_txn_analytics", status="SUCCESS", msg="Pipeline complete", rowcount=n)
dbutils.notebook.exit(f"transaction_analytics: {n} rows")  # noqa: F821
