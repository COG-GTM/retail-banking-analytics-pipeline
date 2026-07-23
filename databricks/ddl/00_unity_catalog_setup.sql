-- =============================================================================
-- Ticket 1 - Unity Catalog foundation
-- =============================================================================
-- Creates the single Unity Catalog catalog and the four schemas that replace the
-- legacy Teradata databases:
--   CORE_BANKING_DB   -> ${catalog}.${core_schema}
--   TXN_PROCESSING_DB -> ${catalog}.${txn_schema}
--   ETL_STAGING_DB    -> ${catalog}.${staging_schema}
--   DATA_PRODUCTS_DB  -> ${catalog}.${products_schema}
-- Placeholders (${...}) are substituted by common.ddl.run_sql_file.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${core_schema}
    COMMENT 'Operational customer/account/address data (was CORE_BANKING_DB)';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${txn_schema}
    COMMENT 'Transaction processing tables (was TXN_PROCESSING_DB)';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${staging_schema}
    COMMENT 'BTEQ-equivalent staging / intermediate tables (was ETL_STAGING_DB)';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${products_schema}
    COMMENT 'Certified data products (was DATA_PRODUCTS_DB)';
