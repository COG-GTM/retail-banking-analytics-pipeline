# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - STG_CUSTOMER_360 (Spark SQL)
# MAGIC
# MAGIC Port of `bteq/01_stg_customer_360.bteq`.
# MAGIC
# MAGIC Builds a denormalized customer-360 staging table by joining customer,
# MAGIC account, and address data from the core banking schema.
# MAGIC
# MAGIC | Teradata / BTEQ                         | Databricks                              |
# MAGIC |----------------------------------------|-----------------------------------------|
# MAGIC | `DROP TABLE` + `CREATE TABLE AS ... WITH DATA` | `CREATE OR REPLACE TABLE ... AS` |
# MAGIC | `QUALIFY ROW_NUMBER() OVER (...)`       | `QUALIFY ROW_NUMBER() OVER (...)` (native) |
# MAGIC | `(CURRENT_DATE - DATE_OF_BIRTH)/365.25` | `datediff(current_date(), date_of_birth)/365.25` |
# MAGIC | `MONTHS_BETWEEN(...)`                   | `months_between(...)`                    |
# MAGIC | `.IF ERRORCODE` / `.IF ACTIVITYCOUNT`  | `validate_table()` + exceptions         |
# MAGIC | `INSERT INTO ETL_RUN_LOG`              | `log_step()` -> Delta audit table       |

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

ensure_audit_table()
guard("bteq", "01_stg_customer_360")
log_step(step="01_stg_customer_360", status="START", msg="Building STG_CUSTOMER_360")

# COMMAND ----------

# Step 1: Drop + recreate the staging table (CREATE OR REPLACE replaces the
# Teradata DROP TABLE + CREATE TABLE AS ... WITH DATA pattern).
spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {stg("stg_customer_360")} AS
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        CAST(datediff(current_date(), c.date_of_birth) / 365.25 AS SMALLINT)  AS age,
        c.customer_since,
        CAST(months_between(current_date(), c.customer_since) AS INT)          AS tenure_months,
        c.customer_status,
        c.segment_code,
        c.branch_id,
        trim(a.address_line_1) || COALESCE(', ' || trim(a.address_line_2), '') AS primary_address,
        a.city,
        a.state_code,
        a.zip_code,
        acct_agg.num_accounts,
        acct_agg.num_active_accounts,
        acct_agg.has_checking,
        acct_agg.has_savings,
        acct_agg.has_credit,
        acct_agg.has_loan,
        acct_agg.total_balance,
        acct_agg.total_credit_limit,
        CASE
            WHEN acct_agg.total_credit_limit > 0
            THEN CAST(acct_agg.credit_balance / acct_agg.total_credit_limit * 100 AS DECIMAL(5,2))
            ELSE 0.00
        END                                                                    AS credit_utilization_pct,
        current_timestamp()                                                    AS load_ts
    FROM {core("customers")} c
    LEFT JOIN (
        SELECT customer_id, address_line_1, address_line_2, city, state_code, zip_code
        FROM {core("addresses")}
        WHERE address_type = 'HOME'
          AND (expiration_date IS NULL OR expiration_date > current_date())
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY effective_date DESC) = 1
    ) a ON c.customer_id = a.customer_id
    LEFT JOIN (
        SELECT
            customer_id,
            COUNT(*)                                                       AS num_accounts,
            SUM(CASE WHEN account_status = 'O' THEN 1 ELSE 0 END)         AS num_active_accounts,
            MAX(CASE WHEN account_type = 'CHECKING' THEN 'Y' ELSE 'N' END) AS has_checking,
            MAX(CASE WHEN account_type = 'SAVINGS'  THEN 'Y' ELSE 'N' END) AS has_savings,
            MAX(CASE WHEN account_type = 'CREDIT'   THEN 'Y' ELSE 'N' END) AS has_credit,
            MAX(CASE WHEN account_type = 'LOAN'     THEN 'Y' ELSE 'N' END) AS has_loan,
            SUM(COALESCE(current_balance, 0))                              AS total_balance,
            SUM(CASE WHEN account_type = 'CREDIT' THEN COALESCE(credit_limit, 0)
                     ELSE 0 END)                                           AS total_credit_limit,
            SUM(CASE WHEN account_type = 'CREDIT' THEN COALESCE(current_balance, 0)
                     ELSE 0 END)                                           AS credit_balance
        FROM {core("accounts")}
        GROUP BY customer_id
    ) acct_agg ON c.customer_id = acct_agg.customer_id
    WHERE c.customer_status IN ('A', 'I')
    """
)

# COMMAND ----------

# Step 2: validation (replaces .IF ACTIVITYCOUNT = 0 THEN .EXIT 99)
rc = validate_table(
    table=stg("stg_customer_360"),
    key_cols=["customer_id"],
    not_null=["customer_id"],
    min_rows=1,
)
if rc != 0:
    log_step(step="01_stg_customer_360", status="ERROR", msg="Validation failed - aborting")
    raise Exception("Validation failed for stg_customer_360")

# COMMAND ----------

# Step 3: log completion (replaces INSERT INTO ETL_RUN_LOG)
n = spark.table(stg("stg_customer_360")).count()  # noqa: F821
log_step(step="01_stg_customer_360", status="SUCCESS", msg="Full load complete", rowcount=n)
dbutils.notebook.exit(f"stg_customer_360: {n} rows")  # noqa: F821
