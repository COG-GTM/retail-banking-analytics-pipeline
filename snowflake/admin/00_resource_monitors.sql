-- =============================================================================
-- Snowflake Resource Monitors - Retail Banking Analytics Pipeline
-- Ticket: MBA-2203 (TICKET-02)
-- =============================================================================
-- Re-runnable. Executed with role ACCOUNTADMIN (resource monitors are an
-- account-level object and cannot be created by a custom role).
--
-- Credit quotas are per calendar month and reset automatically. Values are
-- passed as SnowSQL variables so the same script serves DEV / UAT / PROD:
--   &{env}                environment suffix (DEV|UAT|PROD)
--   &{quota_elt}          monthly credit quota for the ELT warehouse
--   &{quota_spark}        monthly credit quota for the Synapse Spark warehouse
--   &{quota_adhoc}        monthly credit quota for the ad-hoc warehouse
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- ELT transformation workload
-- -----------------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS RM_RB_ELT_&{env}
    WITH CREDIT_QUOTA = &{quota_elt}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75  PERCENT DO NOTIFY
        ON 90  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER RESOURCE MONITOR RM_RB_ELT_&{env} SET CREDIT_QUOTA = &{quota_elt};

-- -----------------------------------------------------------------------------
-- Azure Synapse Spark read/write workload
-- -----------------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS RM_RB_SPARK_&{env}
    WITH CREDIT_QUOTA = &{quota_spark}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75  PERCENT DO NOTIFY
        ON 90  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER RESOURCE MONITOR RM_RB_SPARK_&{env} SET CREDIT_QUOTA = &{quota_spark};

-- -----------------------------------------------------------------------------
-- Ad-hoc analytics workload (business analysts, BI tools)
-- -----------------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS RM_RB_ADHOC_&{env}
    WITH CREDIT_QUOTA = &{quota_adhoc}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER RESOURCE MONITOR RM_RB_ADHOC_&{env} SET CREDIT_QUOTA = &{quota_adhoc};
