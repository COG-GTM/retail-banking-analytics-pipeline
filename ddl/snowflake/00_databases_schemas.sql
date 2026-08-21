-- =============================================================================
-- Snowflake Database & Schema Layout - Retail Banking Analytics
-- =============================================================================
-- Migrated from the Teradata database references in config/pipeline_config.cfg.
--
-- Teradata used four separate databases; Snowflake models them as four schemas
-- inside one environment-suffixed database, so DEV/UAT/PROD are fully isolated
-- and cross-schema joins stay inside a single database:
--
--   CORE_BANKING_DB     -> RETAIL_BANKING_<ENV>.CORE_BANKING
--   TXN_PROCESSING_DB   -> RETAIL_BANKING_<ENV>.TXN_PROCESSING
--   ETL_STAGING_DB      -> RETAIL_BANKING_<ENV>.ETL_STAGING
--   DATA_PRODUCTS_DB    -> RETAIL_BANKING_<ENV>.DATA_PRODUCTS
--
-- Usage (snowsql / Snowsight worksheet): set ENV below, or override it from the
-- caller with `snowsql -D ENV=UAT` / an earlier `SET ENV = 'UAT';`.
--
-- Every statement is idempotent and safe to re-run in any environment.
-- =============================================================================

SET ENV = 'DEV';   -- DEV | UAT | PROD
SET DB_RETAIL = 'RETAIL_BANKING_' || $ENV;

CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_RETAIL)
    COMMENT = 'Retail banking analytics pipeline';

USE DATABASE IDENTIFIER($DB_RETAIL);

CREATE SCHEMA IF NOT EXISTS CORE_BANKING
    COMMENT = 'Replaces Teradata CORE_BANKING_DB: operational customer/account data';

CREATE SCHEMA IF NOT EXISTS TXN_PROCESSING
    COMMENT = 'Replaces Teradata TXN_PROCESSING_DB: transaction processing tables';

CREATE SCHEMA IF NOT EXISTS ETL_STAGING
    COMMENT = 'Replaces Teradata ETL_STAGING_DB: pipeline staging/intermediate tables';

CREATE SCHEMA IF NOT EXISTS DATA_PRODUCTS
    COMMENT = 'Replaces Teradata DATA_PRODUCTS_DB: certified analytical data products';

-- Snowflake has no PRIMARY INDEX / fallback / manual COLLECT STATISTICS.
-- Storage distribution, replication and statistics are managed by the service;
-- clustering keys (declared on the large tables only) replace RANGE_N/COLUMN
-- partitioning where the access pattern justifies it.
