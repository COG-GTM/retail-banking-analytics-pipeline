-- =============================================================================
-- Snowflake Database and Schema Layout - Retail Banking Customer Analytics
-- =============================================================================
-- Migrated from Teradata (see ddl/00_source_tables.sql, ddl/01_staging_tables.sql,
-- ddl/02_data_product_tables.sql for the original definitions).
--
-- Teradata "databases" are containers of tables and are mapped to Snowflake
-- SCHEMAS inside a single per-environment database:
--
--   CORE_BANKING_DB     -> RETAIL_BANKING_<ENV>.CORE_BANKING
--   TXN_PROCESSING_DB   -> RETAIL_BANKING_<ENV>.TXN_PROCESSING
--   ETL_STAGING_DB      -> RETAIL_BANKING_<ENV>.ETL_STAGING
--   DATA_PRODUCTS_DB    -> RETAIL_BANKING_<ENV>.DATA_PRODUCTS
--
-- <ENV> is one of DEV, UAT, PROD and is supplied at run time through the
-- SnowSQL variable `env`:
--
--   snowsql -f ddl/snowflake/00_databases_and_schemas.sql -D env=DEV
--
-- All statements are idempotent and safe to re-run in any environment.
-- Warehouses, roles and grants are provisioned separately (TICKET-02); this
-- script only creates the database/schema containers the DDL depends on.
-- =============================================================================

!set variable_substitution=true

CREATE DATABASE IF NOT EXISTS RETAIL_BANKING_&{env}
    COMMENT = 'Retail banking customer analytics platform (&{env})';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_&{env}.CORE_BANKING
    COMMENT = 'Operational customer/account data (was CORE_BANKING_DB in Teradata)';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_&{env}.TXN_PROCESSING
    COMMENT = 'Transaction processing tables (was TXN_PROCESSING_DB in Teradata)';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_&{env}.ETL_STAGING
    COMMENT = 'ETL staging/intermediate tables (was ETL_STAGING_DB in Teradata)';

CREATE SCHEMA IF NOT EXISTS RETAIL_BANKING_&{env}.DATA_PRODUCTS
    COMMENT = 'Certified analytical data products (was DATA_PRODUCTS_DB in Teradata)';
