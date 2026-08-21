-- =============================================================================
-- Snowflake DDL - DATA_PRODUCTS.TRANSACTION_ANALYTICS   (MBA-2208 / TICKET-07)
-- =============================================================================
-- Migrated from the Teradata definition in ddl/02_data_product_tables.sql.
-- Produced by the Synapse Spark job synapse/spark/jobs/txn_analytics_job.py,
-- which replaces sas/02_sas_txn_analytics.sas.
--
-- Teradata -> Snowflake type mapping used here:
--   BIGINT                      -> NUMBER(19,0)
--   SMALLINT                    -> NUMBER(5,0)
--   INTEGER                     -> NUMBER(10,0)
--   DECIMAL(p,s)                -> NUMBER(p,s)
--   VARCHAR(n) CHARACTER SET LATIN NOT CASESPECIFIC -> VARCHAR(n)
--                               (Snowflake is case-sensitive on comparison;
--                                the pipeline writes canonical upper-case codes)
--   CHAR(1) DEFAULT 'N'         -> CHAR(1) DEFAULT 'N'
--   DATE FORMAT 'YYYY-MM-DD'    -> DATE (formatting is a display concern)
--   TIMESTAMP(6)                -> TIMESTAMP_NTZ(6)
--   PRIMARY INDEX (CUSTOMER_ID) -> CLUSTER BY (REPORTING_PERIOD, CUSTOMER_ID)
--   PARTITION BY COLUMN(REPORTING_PERIOD) -> covered by the clustering key
--                               (Snowflake micro-partitions automatically)
-- =============================================================================

CREATE TABLE IF NOT EXISTS DATA_PRODUCTS.TRANSACTION_ANALYTICS
(
    CUSTOMER_ID                 NUMBER(19,0)    NOT NULL,
    REPORTING_PERIOD            VARCHAR(7),                      -- YYYY-MM
    TOTAL_ACCOUNTS              NUMBER(5,0),
    ACTIVE_ACCOUNTS             NUMBER(5,0),
    TOTAL_TRANSACTIONS          NUMBER(10,0),
    TOTAL_DEBIT_AMT             NUMBER(18,2),
    TOTAL_CREDIT_AMT            NUMBER(18,2),
    NET_CASH_FLOW               NUMBER(18,2),
    AVG_TRANSACTION_SIZE        NUMBER(15,2),
    MONTHLY_SPEND_TREND         VARCHAR(10),                     -- UP, DOWN, STABLE
    SPEND_PERCENTILE            NUMBER(5,2),                     -- PROC RANK GROUPS=100 bucket (0-99)
    TOP_SPEND_CATEGORY          VARCHAR(60),
    DIGITAL_TXN_PCT             NUMBER(5,2),
    FEE_INCOME                  NUMBER(15,2),
    INTEREST_INCOME             NUMBER(15,2),
    REVENUE_CONTRIBUTION        NUMBER(15,2),
    ANOMALY_FLAG                CHAR(1)         DEFAULT 'N',
    MODEL_VERSION               VARCHAR(20),
    EFFECTIVE_DATE              DATE,
    LOAD_TS                     TIMESTAMP_NTZ(6)
)
CLUSTER BY (REPORTING_PERIOD, CUSTOMER_ID)
COMMENT = 'Per-customer transaction behaviour summary with trend indicators. Refresh: daily.';
