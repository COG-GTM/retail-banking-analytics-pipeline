-- =============================================================================
-- Snowflake Staging DDL - STG_CUSTOMER_360 (TICKET-03 / MBA-2204)
-- =============================================================================
-- Snowflake counterpart of the Teradata DDL in ddl/01_staging_tables.sql for
-- the objects owned by TICKET-03. The table itself is created by the dbt model
-- dbt/models/staging/stg_customer_360.sql (materialized = table); this file
-- documents the target column contract and Teradata -> Snowflake type mapping,
-- and creates the shared run-log table the migrated models write to.
--
-- Type mapping applied:
--   BIGINT              -> NUMBER(38,0)
--   SMALLINT / INTEGER  -> NUMBER(5,0) / NUMBER(10,0)
--   VARCHAR(n)          -> VARCHAR(n)
--   CHAR(n)             -> CHAR(n)
--   DATE FORMAT ...     -> DATE (display formatting is a client concern)
--   DECIMAL(p,s)        -> NUMBER(p,s)
--   TIMESTAMP(6)        -> TIMESTAMP_NTZ(6)
-- PRIMARY INDEX / NO FALLBACK / COLLECT STATISTICS have no Snowflake equivalent
-- and are dropped; clustering is unnecessary at this data volume.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ETL_STAGING_DB.STAGING;

-- -----------------------------------------------------------------------------
-- ETL_STAGING_DB.STAGING.STG_CUSTOMER_360
-- Populated by: dbt model stg_customer_360 (was bteq/01_stg_customer_360.bteq)
-- Consumed by:  sas/01_sas_customer_segments.sas (and its migrated successor)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ETL_STAGING_DB.STAGING.STG_CUSTOMER_360
(
    CUSTOMER_ID             NUMBER(38,0)    NOT NULL,
    FIRST_NAME              VARCHAR(60),
    LAST_NAME               VARCHAR(60),
    DATE_OF_BIRTH           DATE,
    AGE                     NUMBER(5,0),
    CUSTOMER_SINCE          DATE,
    TENURE_MONTHS           NUMBER(10,0),
    CUSTOMER_STATUS         CHAR(1),
    SEGMENT_CODE            VARCHAR(10),
    BRANCH_ID               NUMBER(10,0),
    PRIMARY_ADDRESS         VARCHAR(200),
    CITY                    VARCHAR(60),
    STATE_CODE              CHAR(2),
    ZIP_CODE                VARCHAR(10),
    NUM_ACCOUNTS            NUMBER(5,0),
    NUM_ACTIVE_ACCOUNTS     NUMBER(5,0),
    HAS_CHECKING            CHAR(1)         DEFAULT 'N',
    HAS_SAVINGS             CHAR(1)         DEFAULT 'N',
    HAS_CREDIT              CHAR(1)         DEFAULT 'N',
    HAS_LOAN                CHAR(1)         DEFAULT 'N',
    TOTAL_BALANCE           NUMBER(18,2),
    TOTAL_CREDIT_LIMIT      NUMBER(18,2),
    CREDIT_UTILIZATION_PCT  NUMBER(5,2),
    LOAD_TS                 TIMESTAMP_NTZ(6)
);

-- -----------------------------------------------------------------------------
-- ETL_STAGING_DB.PUBLIC.ETL_RUN_LOG
-- Shared run-log replacing the BTEQ ETL_RUN_LOG audit inserts. Written by the
-- log_etl_run() dbt macro as a post-hook on each migrated model.
-- Created here idempotently so TICKET-03 does not depend on TICKET-01 landing
-- first; the CREATE IF NOT EXISTS is a no-op once the shared DDL ships.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ETL_STAGING_DB.PUBLIC.ETL_RUN_LOG
(
    JOB_NAME                VARCHAR(100)    NOT NULL,
    STEP_NAME               VARCHAR(100),
    STATUS                  VARCHAR(20),
    ROW_COUNT               NUMBER(38,0),
    START_TS                TIMESTAMP_NTZ(6),
    END_TS                  TIMESTAMP_NTZ(6),
    DURATION_SEC            NUMBER(38,0)
);
