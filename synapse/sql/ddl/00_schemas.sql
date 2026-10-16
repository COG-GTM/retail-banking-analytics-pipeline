-- =============================================================================
-- AZSYN-01: Synapse dedicated SQL pool schemas
-- =============================================================================
-- Teradata database -> Synapse schema (AZSYN contract v1)
--   CORE_BANKING_DB   -> core
--   TXN_PROCESSING_DB -> txn
--   ETL_STAGING_DB    -> stg
--   DATA_PRODUCTS_DB  -> dp
-- Idempotent. CREATE SCHEMA must be the only statement in its batch, so it is
-- wrapped in EXEC.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'txn')  EXEC('CREATE SCHEMA txn');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')  EXEC('CREATE SCHEMA stg');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dp')   EXEC('CREATE SCHEMA dp');
GO
