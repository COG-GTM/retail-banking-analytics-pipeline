-- =============================================================================
-- Snowflake Grant Hierarchy - Retail Banking Analytics Pipeline
-- Ticket: MBA-2203 (TICKET-02)
-- =============================================================================
-- Re-runnable: GRANT statements are idempotent in Snowflake.
--
-- Assumes the databases and schemas created by TICKET-01 (MBA-2202). That work
-- may not be merged yet, so the database names are SnowSQL variables and
-- default to the Teradata names carried over one-for-one:
--
--   &{db_core}  CORE_BANKING_DB     source: CUSTOMERS, ACCOUNTS, ADDRESSES
--   &{db_txn}   TXN_PROCESSING_DB   source: TRANSACTIONS, TRANSACTION_TYPES
--   &{db_stg}   ETL_STAGING_DB      STG_CUSTOMER_360 / TXN_SUMMARY / RISK_FACTORS
--   &{db_dp}    DATA_PRODUCTS_DB    CUSTOMER_SEGMENTS, TRANSACTION_ANALYTICS,
--                                   CUSTOMER_RISK_SCORES, CUSTOMER_MASTER_PROFILE
--
-- Schema-level grants use ALL SCHEMAS IN DATABASE plus FUTURE SCHEMAS so the
-- script does not need to know TICKET-01's schema layout and stays correct as
-- new schemas are added.
-- =============================================================================

USE ROLE SECURITYADMIN;

-- -----------------------------------------------------------------------------
-- Warehouse usage
-- -----------------------------------------------------------------------------
GRANT USAGE, OPERATE ON WAREHOUSE WH_RB_ELT_&{env}   TO ROLE RB_LOADER_&{env};
GRANT USAGE, OPERATE ON WAREHOUSE WH_RB_ELT_&{env}   TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE, OPERATE ON WAREHOUSE WH_RB_SPARK_&{env} TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE           ON WAREHOUSE WH_RB_ADHOC_&{env} TO ROLE RB_ANALYST_&{env};
GRANT MONITOR         ON WAREHOUSE WH_RB_ELT_&{env}   TO ROLE RB_ADMIN_&{env};
GRANT MONITOR         ON WAREHOUSE WH_RB_SPARK_&{env} TO ROLE RB_ADMIN_&{env};
GRANT MONITOR         ON WAREHOUSE WH_RB_ADHOC_&{env} TO ROLE RB_ADMIN_&{env};

-- -----------------------------------------------------------------------------
-- RB_LOADER: writes the landing/staging layer, reads the operational sources.
-- Intentionally receives NO privilege on &{db_dp}.
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE &{db_core} TO ROLE RB_LOADER_&{env};
GRANT USAGE ON DATABASE &{db_txn}  TO ROLE RB_LOADER_&{env};
GRANT USAGE ON DATABASE &{db_stg}  TO ROLE RB_LOADER_&{env};

GRANT USAGE ON ALL SCHEMAS IN DATABASE &{db_core} TO ROLE RB_LOADER_&{env};
GRANT USAGE ON ALL SCHEMAS IN DATABASE &{db_txn}  TO ROLE RB_LOADER_&{env};
GRANT USAGE ON ALL SCHEMAS IN DATABASE &{db_stg}  TO ROLE RB_LOADER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_core} TO ROLE RB_LOADER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_txn}  TO ROLE RB_LOADER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_stg}  TO ROLE RB_LOADER_&{env};

GRANT SELECT ON ALL TABLES    IN DATABASE &{db_core} TO ROLE RB_LOADER_&{env};
GRANT SELECT ON ALL TABLES    IN DATABASE &{db_txn}  TO ROLE RB_LOADER_&{env};
GRANT SELECT ON FUTURE TABLES IN DATABASE &{db_core} TO ROLE RB_LOADER_&{env};
GRANT SELECT ON FUTURE TABLES IN DATABASE &{db_txn}  TO ROLE RB_LOADER_&{env};

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON ALL SCHEMAS IN DATABASE &{db_stg} TO ROLE RB_LOADER_&{env};
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON FUTURE SCHEMAS IN DATABASE &{db_stg} TO ROLE RB_LOADER_&{env};
GRANT INSERT, UPDATE, DELETE, TRUNCATE, SELECT
    ON ALL TABLES IN DATABASE &{db_stg} TO ROLE RB_LOADER_&{env};
GRANT INSERT, UPDATE, DELETE, TRUNCATE, SELECT
    ON FUTURE TABLES IN DATABASE &{db_stg} TO ROLE RB_LOADER_&{env};

-- -----------------------------------------------------------------------------
-- RB_TRANSFORMER: reads sources and staging, owns the data product layer.
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE &{db_core} TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON DATABASE &{db_txn}  TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON DATABASE &{db_dp}   TO ROLE RB_TRANSFORMER_&{env};

GRANT USAGE ON ALL SCHEMAS    IN DATABASE &{db_core} TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON ALL SCHEMAS    IN DATABASE &{db_txn}  TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON ALL SCHEMAS    IN DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON ALL SCHEMAS    IN DATABASE &{db_dp}   TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_core} TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_txn}  TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_dp}   TO ROLE RB_TRANSFORMER_&{env};

GRANT SELECT ON ALL TABLES     IN DATABASE &{db_core} TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON ALL TABLES     IN DATABASE &{db_txn}  TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON ALL TABLES     IN DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON ALL VIEWS      IN DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON FUTURE TABLES  IN DATABASE &{db_core} TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON FUTURE TABLES  IN DATABASE &{db_txn}  TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON FUTURE TABLES  IN DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};
GRANT SELECT ON FUTURE VIEWS   IN DATABASE &{db_stg}  TO ROLE RB_TRANSFORMER_&{env};

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON ALL SCHEMAS IN DATABASE &{db_dp} TO ROLE RB_TRANSFORMER_&{env};
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON FUTURE SCHEMAS IN DATABASE &{db_dp} TO ROLE RB_TRANSFORMER_&{env};
GRANT INSERT, UPDATE, DELETE, TRUNCATE, SELECT
    ON ALL TABLES IN DATABASE &{db_dp} TO ROLE RB_TRANSFORMER_&{env};
GRANT INSERT, UPDATE, DELETE, TRUNCATE, SELECT
    ON FUTURE TABLES IN DATABASE &{db_dp} TO ROLE RB_TRANSFORMER_&{env};

-- Staging rebuilds: the transformer replaces staging tables it did not create.
GRANT INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN DATABASE &{db_stg} TO ROLE RB_TRANSFORMER_&{env};
GRANT INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN DATABASE &{db_stg} TO ROLE RB_TRANSFORMER_&{env};
GRANT CREATE TABLE, CREATE VIEW
    ON ALL SCHEMAS IN DATABASE &{db_stg} TO ROLE RB_TRANSFORMER_&{env};
GRANT CREATE TABLE, CREATE VIEW
    ON FUTURE SCHEMAS IN DATABASE &{db_stg} TO ROLE RB_TRANSFORMER_&{env};

-- -----------------------------------------------------------------------------
-- RB_ANALYST: read-only on the data product layer only.
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};
GRANT USAGE ON ALL SCHEMAS    IN DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};
GRANT SELECT ON ALL TABLES     IN DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};
GRANT SELECT ON ALL VIEWS      IN DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};
GRANT SELECT ON FUTURE TABLES  IN DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};
GRANT SELECT ON FUTURE VIEWS   IN DATABASE &{db_dp} TO ROLE RB_ANALYST_&{env};

-- -----------------------------------------------------------------------------
-- RB_ADMIN: monitoring and grant management over the whole pipeline footprint.
-- -----------------------------------------------------------------------------
GRANT USAGE, MONITOR ON DATABASE &{db_stg} TO ROLE RB_ADMIN_&{env};
GRANT USAGE, MONITOR ON DATABASE &{db_dp}  TO ROLE RB_ADMIN_&{env};
GRANT MONITOR ON ALL SCHEMAS    IN DATABASE &{db_stg} TO ROLE RB_ADMIN_&{env};
GRANT MONITOR ON ALL SCHEMAS    IN DATABASE &{db_dp}  TO ROLE RB_ADMIN_&{env};
GRANT MONITOR ON FUTURE SCHEMAS IN DATABASE &{db_stg} TO ROLE RB_ADMIN_&{env};
GRANT MONITOR ON FUTURE SCHEMAS IN DATABASE &{db_dp}  TO ROLE RB_ADMIN_&{env};
