-- =============================================================================
-- Snowflake Warehouses - Retail Banking Analytics Pipeline
-- Ticket: MBA-2203 (TICKET-02)
-- =============================================================================
-- Re-runnable: CREATE ... IF NOT EXISTS establishes the object, the following
-- ALTER re-applies the desired-state configuration on every run so drift is
-- corrected rather than ignored.
--
-- Sizing rationale (replaces the single Teradata production system that ran
-- BTEQ staging, SAS analytics and ad-hoc queries on shared AMPs):
--
--   WH_RB_ELT_<env>     MEDIUM   BTEQ -> SQL staging transformations
--                                (STG_CUSTOMER_360 / STG_TXN_SUMMARY /
--                                 STG_RISK_FACTORS), batch, latency tolerant.
--   WH_RB_SPARK_<env>   LARGE    Azure Synapse Spark read/write of the
--                                migrated SAS analytics; multi-cluster so
--                                concurrent notebook executors queue less.
--   WH_RB_ADHOC_<env>   SMALL    Analyst / BI read-only queries against the
--                                data product layer.
--
-- Variables: &{env}
-- =============================================================================

USE ROLE SYSADMIN;

-- -----------------------------------------------------------------------------
-- ELT transformation warehouse
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_RB_ELT_&{env}
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY'
    STATEMENT_TIMEOUT_IN_SECONDS = 7200
    COMMENT = 'Batch ELT transformations migrated from Teradata BTEQ (MBA-2203)';

ALTER WAREHOUSE WH_RB_ELT_&{env} SET
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY'
    STATEMENT_TIMEOUT_IN_SECONDS = 7200;

-- -----------------------------------------------------------------------------
-- Azure Synapse Spark warehouse
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_RB_SPARK_&{env}
    WAREHOUSE_SIZE = 'LARGE'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    STATEMENT_TIMEOUT_IN_SECONDS = 10800
    COMMENT = 'Synapse Spark read/write for migrated SAS analytics (MBA-2203)';

ALTER WAREHOUSE WH_RB_SPARK_&{env} SET
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    STATEMENT_TIMEOUT_IN_SECONDS = 10800;

-- -----------------------------------------------------------------------------
-- Ad-hoc analytics warehouse
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_RB_ADHOC_&{env}
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY'
    STATEMENT_TIMEOUT_IN_SECONDS = 1800
    COMMENT = 'Ad-hoc analyst and BI access to data products (MBA-2203)';

ALTER WAREHOUSE WH_RB_ADHOC_&{env} SET
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY'
    STATEMENT_TIMEOUT_IN_SECONDS = 1800;

-- -----------------------------------------------------------------------------
-- Attach resource monitors (requires ACCOUNTADMIN)
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

ALTER WAREHOUSE WH_RB_ELT_&{env}   SET RESOURCE_MONITOR = RM_RB_ELT_&{env};
ALTER WAREHOUSE WH_RB_SPARK_&{env} SET RESOURCE_MONITOR = RM_RB_SPARK_&{env};
ALTER WAREHOUSE WH_RB_ADHOC_&{env} SET RESOURCE_MONITOR = RM_RB_ADHOC_&{env};
