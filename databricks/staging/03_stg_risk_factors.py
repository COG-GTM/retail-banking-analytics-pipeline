# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - STG_RISK_FACTORS (Spark SQL)
# MAGIC
# MAGIC Port of `bteq/03_stg_risk_factors.bteq`.
# MAGIC
# MAGIC Computes per-customer risk feature vectors (balance behaviour, payment
# MAGIC history, transaction velocity, merchant risk, external bureau score).
# MAGIC
# MAGIC | Teradata / BTEQ                              | Databricks                                  |
# MAGIC |---------------------------------------------|---------------------------------------------|
# MAGIC | `WRK_DAILY_BALANCE` / `WRK_PAYMENT_HISTORY` Delta work tables | temporary views          |
# MAGIC | `ADD_MONTHS(CURRENT_DATE, -n)`              | `add_months(current_date(), -n)`            |
# MAGIC | `CURRENT_DATE - n`                          | `date_sub(current_date(), n)`               |
# MAGIC | `MONTHS_BETWEEN(...) (INTEGER)`             | `CAST(months_between(...) AS INT)`          |
# MAGIC | `STDDEV_POP(...)`                           | `stddev_pop(...)`                           |
# MAGIC | `QUALIFY ROW_NUMBER() OVER (...)`           | `QUALIFY ROW_NUMBER() OVER (...)` (native)  |

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

ensure_audit_table()
guard("bteq", "03_stg_risk_factors")
log_step(step="03_stg_risk_factors", status="START", msg="Building STG_RISK_FACTORS")

# COMMAND ----------

# Intermediate 1: daily balance snapshots (last 3 months) -> temp view
spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TEMPORARY VIEW wrk_daily_balance AS
    SELECT acct.customer_id, t.account_id, t.transaction_date, t.running_balance AS eod_balance
    FROM {txn("transactions")} t
    JOIN {core("accounts")} acct ON t.account_id = acct.account_id
    WHERE t.transaction_date >= add_months(current_date(), -3)
      AND t.status_code = 'P'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.account_id, t.transaction_date
        ORDER BY t.transaction_ts DESC) = 1
    """
)

# COMMAND ----------

# Intermediate 2: payment behaviour on credit / loan accounts -> temp view
spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TEMPORARY VIEW wrk_payment_history AS
    SELECT
        acct.customer_id,
        acct.account_id,
        COUNT(*)                                                AS total_payments,
        SUM(CASE WHEN t.transaction_date <= add_months(
                 acct.open_date, CAST(months_between(t.transaction_date, acct.open_date) AS INT) + 1)
             THEN 1 ELSE 0 END)                                 AS ontime_payments,
        SUM(CASE WHEN t.transaction_date > add_months(
                 acct.open_date, CAST(months_between(t.transaction_date, acct.open_date) AS INT) + 1)
             THEN 1 ELSE 0 END)                                 AS late_payments,
        CAST(months_between(
             current_date(),
             COALESCE(MAX(CASE
                 WHEN t.transaction_date > add_months(
                      acct.open_date, CAST(months_between(t.transaction_date, acct.open_date) AS INT) + 1)
                 THEN t.transaction_date END), MIN(acct.open_date))
        ) AS INT)                                               AS months_since_last_late
    FROM {txn("transactions")} t
    JOIN {core("accounts")} acct ON t.account_id = acct.account_id
    JOIN {txn("transaction_types")} tt ON t.transaction_type_cd = tt.transaction_type_cd
    WHERE acct.account_type IN ('CREDIT', 'LOAN')
      AND tt.category = 'CREDIT'
      AND t.status_code = 'P'
      AND t.transaction_date >= add_months(current_date(), -24)
    GROUP BY acct.customer_id, acct.account_id
    """
)

# COMMAND ----------

# Final risk factors table
spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {stg("stg_risk_factors")} AS
    SELECT
        c.customer_id,
        COALESCE(overdraft.overdraft_count, 0)     AS account_overdraft_cnt,
        COALESCE(overdraft.nsf_total, 0.00)        AS nsf_fee_total,
        COALESCE(lg_wd.large_wd_cnt, 0)            AS large_withdrawal_cnt,
        COALESCE(lg_wd.large_wd_amt, 0.00)         AS large_withdrawal_amt,
        COALESCE(bal.avg_bal_30d, 0.00)            AS avg_daily_balance_30d,
        COALESCE(bal.avg_bal_90d, 0.00)            AS avg_daily_balance_90d,
        COALESCE(bal.bal_stddev, 0.0000)           AS balance_volatility,
        CASE WHEN credit.total_credit_limit > 0
             THEN CAST(credit.total_credit_bal / credit.total_credit_limit AS DECIMAL(5,4))
             ELSE 0.0000 END                       AS credit_util_ratio,
        CASE WHEN pmh.total_payments > 0
             THEN CAST(pmh.ontime_payments * 100.0 / pmh.total_payments AS DECIMAL(5,2))
             ELSE 100.00 END                       AS payment_ontime_pct,
        COALESCE(pmh.late_payments, 0)             AS payment_late_cnt,
        COALESCE(pmh.months_since_last_late, 999)  AS months_since_last_late,
        COALESCE(bureau.credit_score, 0)           AS external_credit_score,
        COALESCE(vel.debit_7d, 0.00)               AS debit_velocity_7d,
        COALESCE(vel.debit_30d, 0.00)              AS debit_velocity_30d,
        COALESCE(merch.new_merch_30d, 0)           AS new_merchant_cnt_30d,
        COALESCE(merch.intl_txn_cnt, 0)            AS international_txn_cnt,
        COALESCE(merch.high_risk_cnt, 0)           AS high_risk_merchant_cnt,
        current_timestamp()                        AS load_ts
    FROM {core("customers")} c

    LEFT JOIN (
        SELECT acct.customer_id,
            SUM(CASE WHEN t.running_balance < 0 THEN 1 ELSE 0 END)     AS overdraft_count,
            SUM(CASE WHEN tt.category = 'FEE' AND tt.description LIKE '%NSF%'
                     THEN ABS(t.amount) ELSE 0 END)                     AS nsf_total
        FROM {txn("transactions")} t
        JOIN {core("accounts")} acct ON t.account_id = acct.account_id
        JOIN {txn("transaction_types")} tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE t.transaction_date >= add_months(current_date(), -12) AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ) overdraft ON c.customer_id = overdraft.customer_id

    LEFT JOIN (
        SELECT acct.customer_id, COUNT(*) AS large_wd_cnt, SUM(ABS(t.amount)) AS large_wd_amt
        FROM {txn("transactions")} t
        JOIN {core("accounts")} acct ON t.account_id = acct.account_id
        JOIN {txn("transaction_types")} tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE tt.category = 'DEBIT' AND ABS(t.amount) >= 5000
          AND t.transaction_date >= add_months(current_date(), -12) AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ) lg_wd ON c.customer_id = lg_wd.customer_id

    LEFT JOIN (
        SELECT customer_id,
            AVG(CASE WHEN transaction_date >= date_sub(current_date(), 30) THEN eod_balance END) AS avg_bal_30d,
            AVG(CASE WHEN transaction_date >= date_sub(current_date(), 90) THEN eod_balance END) AS avg_bal_90d,
            stddev_pop(eod_balance) AS bal_stddev
        FROM wrk_daily_balance GROUP BY customer_id
    ) bal ON c.customer_id = bal.customer_id

    LEFT JOIN (
        SELECT customer_id,
            SUM(COALESCE(current_balance, 0)) AS total_credit_bal,
            SUM(COALESCE(credit_limit, 0))    AS total_credit_limit
        FROM {core("accounts")}
        WHERE account_type = 'CREDIT' AND account_status = 'O'
        GROUP BY customer_id
    ) credit ON c.customer_id = credit.customer_id

    LEFT JOIN (
        SELECT customer_id,
            SUM(total_payments)         AS total_payments,
            SUM(ontime_payments)        AS ontime_payments,
            SUM(late_payments)          AS late_payments,
            MIN(months_since_last_late) AS months_since_last_late
        FROM wrk_payment_history GROUP BY customer_id
    ) pmh ON c.customer_id = pmh.customer_id

    LEFT JOIN (
        SELECT customer_id, external_credit_score AS credit_score
        FROM {core("customer_bureau_scores")}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY report_date DESC) = 1
    ) bureau ON c.customer_id = bureau.customer_id

    LEFT JOIN (
        SELECT acct.customer_id,
            SUM(CASE WHEN t.transaction_date >= date_sub(current_date(), 7)
                     THEN ABS(t.amount) ELSE 0 END) AS debit_7d,
            SUM(CASE WHEN t.transaction_date >= date_sub(current_date(), 30)
                     THEN ABS(t.amount) ELSE 0 END) AS debit_30d
        FROM {txn("transactions")} t
        JOIN {core("accounts")} acct ON t.account_id = acct.account_id
        JOIN {txn("transaction_types")} tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE tt.category = 'DEBIT' AND t.transaction_date >= date_sub(current_date(), 30)
          AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ) vel ON c.customer_id = vel.customer_id

    LEFT JOIN (
        SELECT acct.customer_id,
            COUNT(DISTINCT CASE
                WHEN t.transaction_date >= date_sub(current_date(), 30) THEN t.merchant_name END) AS new_merch_30d,
            SUM(CASE WHEN t.channel_code = 'INTL' THEN 1 ELSE 0 END)                  AS intl_txn_cnt,
            SUM(CASE WHEN t.merchant_category IN (
                'GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP')
                     THEN 1 ELSE 0 END)                                               AS high_risk_cnt
        FROM {txn("transactions")} t
        JOIN {core("accounts")} acct ON t.account_id = acct.account_id
        WHERE t.transaction_date >= add_months(current_date(), -6) AND t.status_code = 'P'
        GROUP BY acct.customer_id
    ) merch ON c.customer_id = merch.customer_id

    WHERE c.customer_status IN ('A', 'I')
    """
)

# COMMAND ----------

rc = validate_table(
    table=stg("stg_risk_factors"),
    key_cols=["customer_id"],
    not_null=["customer_id"],
    min_rows=1,
)
if rc != 0:
    log_step(step="03_stg_risk_factors", status="ERROR", msg="Validation failed - aborting")
    raise Exception("Validation failed for stg_risk_factors")

# COMMAND ----------

n = spark.table(stg("stg_risk_factors")).count()  # noqa: F821
log_step(step="03_stg_risk_factors", status="SUCCESS", msg="Full load complete", rowcount=n)
dbutils.notebook.exit(f"stg_risk_factors: {n} rows")  # noqa: F821
