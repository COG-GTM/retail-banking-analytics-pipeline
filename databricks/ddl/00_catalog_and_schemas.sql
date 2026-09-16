-- =============================================================================
-- Catalog, schemas, audit table, and optional Lakehouse Federation connection.
-- Replaces the Teradata database layer (CORE_BANKING_DB, TXN_PROCESSING_DB,
-- ETL_STAGING_DB, DATA_PRODUCTS_DB -> UC schemas).
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog};

CREATE SCHEMA IF NOT EXISTS ${catalog}.core_banking
  COMMENT 'Bronze - operational source data (was CORE_BANKING_DB)';
CREATE SCHEMA IF NOT EXISTS ${catalog}.txn_processing
  COMMENT 'Bronze - transaction data (was TXN_PROCESSING_DB)';
CREATE SCHEMA IF NOT EXISTS ${catalog}.etl_staging
  COMMENT 'Silver - BTEQ staging/intermediate tables (was ETL_STAGING_DB)';
CREATE SCHEMA IF NOT EXISTS ${catalog}.data_products
  COMMENT 'Gold - certified analytical outputs (was DATA_PRODUCTS_DB)';

-- SAS WORK.PIPELINE_AUDIT equivalent
CREATE TABLE IF NOT EXISTS ${catalog}.etl_staging.pipeline_audit (
    job_name    STRING  COMMENT 'Pipeline step name',
    status      STRING  COMMENT 'START | SUCCESS | WARNING | ERROR',
    message     STRING  COMMENT 'Free-text description',
    row_count   BIGINT  COMMENT 'Rows processed',
    log_ts      TIMESTAMP COMMENT 'Log timestamp'
) USING DELTA
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);

-- =============================================================================
-- OPTIONAL: Lakehouse Federation to Teradata (replaces BTEQ logon + LDAP).
-- When the connection exists, bronze can be read directly from the foreign
-- catalog instead of JDBC ingest in 00_bronze_ingest.
-- =============================================================================
-- CREATE CONNECTION IF NOT EXISTS teradata_core TYPE TERADATA
-- OPTIONS (
--     host     secret('retail-banking-teradata', 'td-host'),
--     user     secret('retail-banking-teradata', 'td-user'),
--     password secret('retail-banking-teradata', 'td-password')
-- );
--
-- CREATE FOREIGN CATALOG IF NOT EXISTS teradata_core_catalog
-- USING CONNECTION teradata_core
-- OPTIONS (database 'CORE_BANKING_DB');
