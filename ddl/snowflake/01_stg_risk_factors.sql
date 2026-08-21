-- =============================================================================
-- Snowflake DDL - STG_RISK_FACTORS  (TICKET-05 / MBA-2206)
-- =============================================================================
-- Migrated from the Teradata definition in ddl/01_staging_tables.sql.
-- The table is built by the dbt model dbt/models/staging/stg_risk_factors.sql
-- (materialized as a table); this script documents and, where dbt is not used,
-- provisions the column contract. Idempotent and safe to re-run in DEV/UAT/PROD.
--
-- Teradata -> Snowflake type mapping applied here:
--   BIGINT                -> NUMBER(19,0)
--   INTEGER               -> NUMBER(9,0)
--   DECIMAL(p,s)          -> NUMBER(p,s)
--   TIMESTAMP(6)          -> TIMESTAMP_NTZ
-- Removed Teradata-only clauses: MULTISET, NO FALLBACK, PRIMARY INDEX,
-- COLLECT STATISTICS.
--
-- Note: TICKET-01 owns the full DDL migration; if that ticket lands a
-- consolidated ddl/snowflake/01_staging_tables.sql, this file should be folded
-- into it unchanged.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ETL_STAGING.STG_RISK_FACTORS
(
    CUSTOMER_ID             NUMBER(19,0)    NOT NULL,
    ACCOUNT_OVERDRAFT_CNT   NUMBER(9,0),
    NSF_FEE_TOTAL           NUMBER(15,2),
    LARGE_WITHDRAWAL_CNT    NUMBER(9,0),
    LARGE_WITHDRAWAL_AMT    NUMBER(18,2),
    AVG_DAILY_BALANCE_30D   NUMBER(15,2),
    AVG_DAILY_BALANCE_90D   NUMBER(15,2),
    BALANCE_VOLATILITY      NUMBER(10,4),
    CREDIT_UTIL_RATIO       NUMBER(5,4),
    PAYMENT_ONTIME_PCT      NUMBER(5,2),
    PAYMENT_LATE_CNT        NUMBER(9,0),
    MONTHS_SINCE_LAST_LATE  NUMBER(9,0),
    EXTERNAL_CREDIT_SCORE   NUMBER(9,0),
    DEBIT_VELOCITY_7D       NUMBER(15,2),
    DEBIT_VELOCITY_30D      NUMBER(15,2),
    NEW_MERCHANT_CNT_30D    NUMBER(9,0),
    INTERNATIONAL_TXN_CNT   NUMBER(9,0),
    HIGH_RISK_MERCHANT_CNT  NUMBER(9,0),
    LOAD_TS                 TIMESTAMP_NTZ
);

-- Run-log table replacing ETL_STAGING_DB.ETL_RUN_LOG audit inserts. Created on
-- demand by the dbt macro log_etl_run(); declared here so a non-dbt deployment
-- has the same contract. Superseded by the shared mechanism from TICKET-03 if
-- that ticket defines one.
CREATE TABLE IF NOT EXISTS ETL_STAGING.ETL_RUN_LOG
(
    JOB_NAME    VARCHAR(100),
    STEP_NAME   VARCHAR(100),
    STATUS      VARCHAR(20),
    ROW_COUNT   NUMBER(18,0),
    START_TS    TIMESTAMP_NTZ,
    END_TS      TIMESTAMP_NTZ
);
