# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - STG_TXN_SUMMARY (Spark SQL)
# MAGIC
# MAGIC Port of `bteq/02_stg_txn_summary.bteq`.
# MAGIC
# MAGIC Aggregates transaction-level data into per-customer/account summary metrics
# MAGIC over a configurable lookback window (`LOOKBACK_MONTHS`, default 12).
# MAGIC
# MAGIC | Teradata / BTEQ                                            | Databricks                          |
# MAGIC |-----------------------------------------------------------|-------------------------------------|
# MAGIC | `CREATE VOLATILE TABLE VT_RUN_PARAMS ... ON COMMIT PRESERVE ROWS` | `run_params` CTE          |
# MAGIC | `ADD_MONTHS(CURRENT_DATE, -LOOKBACK_MONTHS)`              | `add_months(current_date(), -LOOKBACK_MONTHS)` |
# MAGIC | `NULLIFZERO(COUNT(*))`                                     | `nullif(COUNT(*), 0)`               |
# MAGIC | `CURRENT_DATE - MAX(t.TRANSACTION_DATE)`                   | `datediff(current_date(), MAX(...))`|
# MAGIC | top-category `QUALIFY` over windowed SUM                   | `top_cat` CTE (group + ROW_NUMBER)  |

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

ensure_audit_table()
guard("bteq", "02_stg_txn_summary")
log_step(step="02_stg_txn_summary", status="START", msg=f"Lookback: {LOOKBACK_MONTHS} months")

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {stg("stg_txn_summary")} AS
    WITH run_params AS (
        SELECT
            add_months(current_date(), -{LOOKBACK_MONTHS}) AS period_start,
            current_date()                                 AS period_end
    ),
    top_cat AS (
        SELECT account_id, merchant_category
        FROM (
            SELECT
                t2.account_id,
                t2.merchant_category,
                SUM(ABS(t2.amount)) AS cat_spend,
                ROW_NUMBER() OVER (
                    PARTITION BY t2.account_id ORDER BY SUM(ABS(t2.amount)) DESC
                ) AS rn
            FROM {txn("transactions")} t2, run_params rp2
            WHERE t2.transaction_date BETWEEN rp2.period_start AND rp2.period_end
              AND t2.status_code = 'P'
              AND t2.merchant_category IS NOT NULL
            GROUP BY t2.account_id, t2.merchant_category
        ) sub WHERE rn = 1
    )
    SELECT
        acct.customer_id,
        acct.account_id,
        acct.account_type,
        rp.period_start                                              AS summary_period_start,
        rp.period_end                                                AS summary_period_end,
        COUNT(*)                                                     AS txn_count_total,
        SUM(CASE WHEN tt.category = 'DEBIT'  THEN 1 ELSE 0 END)      AS txn_count_debit,
        SUM(CASE WHEN tt.category = 'CREDIT' THEN 1 ELSE 0 END)      AS txn_count_credit,
        SUM(CASE WHEN tt.category = 'FEE'    THEN 1 ELSE 0 END)      AS txn_count_fee,
        SUM(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_total_debit,
        SUM(CASE WHEN tt.category = 'CREDIT' THEN t.amount      ELSE 0 END) AS amt_total_credit,
        SUM(CASE WHEN tt.category = 'FEE'    THEN ABS(t.amount) ELSE 0 END) AS amt_total_fees,
        AVG(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) END)        AS amt_avg_debit,
        AVG(CASE WHEN tt.category = 'CREDIT' THEN t.amount      END)        AS amt_avg_credit,
        MAX(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_max_single_debit,
        MAX(CASE WHEN tt.category = 'CREDIT' THEN t.amount      ELSE 0 END) AS amt_max_single_credit,
        COUNT(DISTINCT t.merchant_name)                             AS distinct_merchants,
        MAX(tc.merchant_category)                                   AS top_merchant_category,
        CAST(SUM(CASE WHEN t.channel_code = 'ATM' THEN 1 ELSE 0 END) * 100.0
             / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_atm,
        CAST(SUM(CASE WHEN t.channel_code = 'POS' THEN 1 ELSE 0 END) * 100.0
             / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_pos,
        CAST(SUM(CASE WHEN t.channel_code = 'WEB' THEN 1 ELSE 0 END) * 100.0
             / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_web,
        CAST(SUM(CASE WHEN t.channel_code = 'MOB' THEN 1 ELSE 0 END) * 100.0
             / nullif(COUNT(*), 0) AS DECIMAL(5,2))                 AS pct_mobile,
        CAST(datediff(current_date(), MAX(t.transaction_date)) AS INT) AS days_since_last_txn,
        current_timestamp()                                         AS load_ts
    FROM {txn("transactions")} t
    JOIN {core("accounts")} acct              ON t.account_id = acct.account_id
    JOIN {txn("transaction_types")} tt        ON t.transaction_type_cd = tt.transaction_type_cd
    CROSS JOIN run_params rp
    LEFT JOIN top_cat tc                       ON t.account_id = tc.account_id
    WHERE t.transaction_date BETWEEN rp.period_start AND rp.period_end
      AND t.status_code = 'P'
    GROUP BY acct.customer_id, acct.account_id, acct.account_type,
             rp.period_start, rp.period_end, tc.merchant_category
    """
)

# COMMAND ----------

rc = validate_table(
    table=stg("stg_txn_summary"),
    key_cols=["customer_id", "account_id"],
    not_null=["customer_id", "account_id"],
    min_rows=1,
)
if rc != 0:
    log_step(step="02_stg_txn_summary", status="ERROR", msg="Validation failed - aborting")
    raise Exception("Validation failed for stg_txn_summary")

# COMMAND ----------

n = spark.table(stg("stg_txn_summary")).count()  # noqa: F821
log_step(step="02_stg_txn_summary", status="SUCCESS", msg="Full load complete", rowcount=n)
dbutils.notebook.exit(f"stg_txn_summary: {n} rows")  # noqa: F821
