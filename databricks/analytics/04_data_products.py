# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Customer Master Profile (Spark SQL)
# MAGIC
# MAGIC Port of `sas/04_sas_data_products.sas`. Mirrors
# MAGIC `local/duckdb/run_demo.py::_phase3d_master_profile`.
# MAGIC
# MAGIC Assembles the golden record by left-joining the base customer-360 staging
# MAGIC table with the three upstream data products. The SAS 4-way MERGE with `IN=`
# MAGIC outer-join semantics becomes Spark `LEFT JOIN` + `coalesce` for the default
# MAGIC handling of missing segment / txn / risk records:
# MAGIC
# MAGIC | Missing source | Defaults applied                                          |
# MAGIC |----------------|-----------------------------------------------------------|
# MAGIC | segment        | UNCLASSIFIED, scores 0, flags N                           |
# MAGIC | txn            | counts/amounts 0, top_spend_category ''                   |
# MAGIC | risk           | risk_tier UNKNOWN, watch_list_flag N (score/PD stay NULL) |

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

ensure_audit_table()
guard("sas", "04_data_products")
log_step(step="04_data_products", status="START", msg="Building golden record")

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {dp("customer_master_profile")} AS
    SELECT
        b.customer_id,
        concat(trim(b.first_name), ' ', trim(b.last_name))       AS full_name,
        b.age,
        b.state_code,
        b.customer_since,
        b.tenure_months,
        b.customer_status,
        -- Segment data
        COALESCE(s.segment_name, 'UNCLASSIFIED')                 AS segment_name,
        COALESCE(s.lifetime_value_score, 0)                      AS lifetime_value_score,
        COALESCE(s.engagement_score, 0)                          AS engagement_score,
        -- Account summary
        b.num_accounts                                           AS total_accounts,
        b.num_active_accounts                                    AS active_accounts,
        b.total_balance,
        b.total_credit_limit,
        b.credit_utilization_pct,
        -- Transaction summary
        COALESCE(t.total_transactions, 0)                        AS monthly_transactions,
        COALESCE(t.total_debit_amt, 0)                           AS monthly_spend,
        COALESCE(t.net_cash_flow, 0)                             AS net_cash_flow,
        COALESCE(t.top_spend_category, '')                       AS top_spend_category,
        COALESCE(t.digital_txn_pct, 0)                           AS digital_txn_pct,
        -- Risk profile (score / PD stay NULL when absent, as in the SAS code)
        r.composite_risk_score,
        COALESCE(r.risk_tier, 'UNKNOWN')                         AS risk_tier,
        r.probability_of_default,
        COALESCE(r.watch_list_flag, 'N')                         AS watch_list_flag,
        -- Actionable flags
        COALESCE(s.cross_sell_flag, 'N')                         AS cross_sell_flag,
        COALESCE(s.upsell_flag, 'N')                             AS upsell_flag,
        COALESCE(s.retention_risk_flag, 'N')                     AS retention_risk_flag,
        -- Metadata
        '{MODEL_VERSION_MASTER}'                                 AS model_version,
        current_date()                                           AS effective_date,
        current_timestamp()                                      AS load_ts
    FROM {stg("stg_customer_360")} b
    LEFT JOIN {dp("customer_segments")}     s ON b.customer_id = s.customer_id
    LEFT JOIN {dp("transaction_analytics")} t ON b.customer_id = t.customer_id
    LEFT JOIN {dp("customer_risk_scores")}  r ON b.customer_id = r.customer_id
    WHERE b.customer_status = 'A'
    """
)

# COMMAND ----------

# Data quality report (mirrors SAS STEP 3)
display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT
            count(*) AS total,
            sum(CASE WHEN segment_name <> 'UNCLASSIFIED' THEN 1 ELSE 0 END) AS has_segment,
            sum(CASE WHEN monthly_transactions > 0       THEN 1 ELSE 0 END) AS has_txn,
            sum(CASE WHEN risk_tier <> 'UNKNOWN'         THEN 1 ELSE 0 END) AS has_risk_score,
            sum(CASE WHEN cross_sell_flag = 'Y'          THEN 1 ELSE 0 END) AS cross_sell_eligible,
            sum(CASE WHEN upsell_flag = 'Y'              THEN 1 ELSE 0 END) AS upsell_eligible,
            sum(CASE WHEN retention_risk_flag = 'Y'      THEN 1 ELSE 0 END) AS retention_at_risk,
            sum(CASE WHEN watch_list_flag = 'Y'          THEN 1 ELSE 0 END) AS on_watch_list
        FROM {dp("customer_master_profile")}
        """
    )
)

# COMMAND ----------

rc = validate_table(
    table=dp("customer_master_profile"),
    key_cols=["customer_id"],
    not_null=["customer_id", "full_name", "customer_status"],
    min_rows=1,
)
if rc != 0:
    log_step(step="04_data_products", status="ERROR", msg="Validation failed")
    raise Exception("Validation failed for customer_master_profile")

n = spark.table(dp("customer_master_profile")).count()  # noqa: F821
log_step(step="04_data_products", status="SUCCESS", msg="Golden record loaded", rowcount=n)
dbutils.notebook.exit(f"customer_master_profile: {n} rows")  # noqa: F821
