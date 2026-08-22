-- =============================================================================
-- Shared pipeline run log
-- =============================================================================
-- Replaces the SAS-side WORK.PIPELINE_AUDIT dataset that %log_step / %init_audit
-- maintained per session. Every Synapse Spark job appends its audit trail here
-- through pipeline_utils.run_log.RunLogger, so the trail survives the Spark
-- session and validation aborts stay visible after the job exits.
--
-- Database/schema naming follows TICKET-01: RETAIL_BANKING_<ENV>.ETL_STAGING.
-- =============================================================================

SET ENV = 'DEV';   -- DEV | UAT | PROD
SET DB_RETAIL = 'RETAIL_BANKING_' || $ENV;

USE DATABASE IDENTIFIER($DB_RETAIL);
USE SCHEMA ETL_STAGING;

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG
(
    RUN_ID      VARCHAR(36)     NOT NULL,
    JOB_NAME    VARCHAR(40)     NOT NULL,
    STEP_NAME   VARCHAR(40)     NOT NULL,
    STATUS      VARCHAR(10)     NOT NULL,  -- START | SUCCESS | WARNING | ERROR
    MESSAGE     VARCHAR(200),
    ROW_COUNT   NUMBER(38,0),
    LOG_TS      TIMESTAMP_NTZ(3) NOT NULL
)
COMMENT = 'Audit trail for the Synapse Spark pipeline jobs (replaces WORK.PIPELINE_AUDIT)';
