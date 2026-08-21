-- =============================================================================
-- Shared pipeline run-log table (Snowflake)
-- =============================================================================
-- Replaces the SAS in-session audit dataset WORK.PIPELINE_AUDIT written by
-- %log_step (sas/macros/log_step.sas). Every Synapse Spark job appends one row
-- per pipeline step through pipeline_utils.run_log.RunLogSink.
--
-- Database/schema names follow the TICKET-01 layout:
--   DATA_PRODUCTS_<ENV>.DATA_PRODUCTS
-- Substitute <ENV> with DEV / UAT / PROD at deploy time.
-- =============================================================================

CREATE TABLE IF NOT EXISTS DATA_PRODUCTS_DEV.DATA_PRODUCTS.PIPELINE_RUN_LOG
(
    RUN_ID      VARCHAR(64),          -- Synapse pipeline run id
    JOB_NAME    VARCHAR(40),          -- pipeline step (SAS %log_step step=)
    STATUS      VARCHAR(10),          -- START | SUCCESS | WARNING | ERROR
    MESSAGE     VARCHAR(200),
    ROW_COUNT   NUMBER(18,0),
    LOG_TS      TIMESTAMP_NTZ
);
