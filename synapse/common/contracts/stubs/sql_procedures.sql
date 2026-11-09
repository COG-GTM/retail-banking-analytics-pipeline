-- =============================================================================
-- Contract stub: T-SQL stored procedure SIGNATURES ONLY (no bodies).
-- Source of truth: C2 (audit), C3 (validation), C7 (entry points).
-- This file is documentation and is NOT deployed. Each proc is implemented in its
-- owner's file as:  IF OBJECT_ID(...) IS NOT NULL DROP PROCEDURE ...; GO; CREATE PROCEDURE ...
-- =============================================================================

-- T9  synapse/common/sql/usp_log_step.sql                                  (C2 §3.1)
-- ETL_STAGING_DB.usp_log_step
--     @RunId VARCHAR(64), @JobName VARCHAR(100), @StepName VARCHAR(100), @Status VARCHAR(10),
--     @RowCount BIGINT = NULL, @Message VARCHAR(4000) = NULL,
--     @StartTs DATETIME2(6) = NULL, @EndTs DATETIME2(6) = NULL,
--     @Env VARCHAR(10) = 'DEV', @AsOfDate DATE = NULL, @Engine VARCHAR(10) = 'SQL'

-- T9  synapse/common/sql/usp_validate_table.sql                            (C3 §4.1)
-- ETL_STAGING_DB.usp_validate_table
--     @RunId VARCHAR(64), @JobName VARCHAR(100), @SchemaName SYSNAME, @TableName SYSNAME,
--     @KeyCols VARCHAR(4000) = NULL, @NotNullCols VARCHAR(4000) = NULL, @MinRows BIGINT = 1,
--     @Env VARCHAR(10) = 'DEV', @AsOfDate DATE = NULL, @RowCount BIGINT = NULL OUTPUT

-- T3  synapse/ingestion/usp_load_source_tables.sql                         (C7 §2)
-- ETL_STAGING_DB.usp_load_source_tables
--     @RunId VARCHAR(64), @Env VARCHAR(10), @LoadDate DATE, @StorageAccount VARCHAR(64),
--     @Tables VARCHAR(4000) = NULL

-- T4  synapse/sql/staging/usp_stg_customer_360.sql                         (C7 §2)
-- ETL_STAGING_DB.usp_stg_customer_360
--     @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL

-- T5  synapse/sql/staging/usp_stg_txn_summary.sql                          (C7 §2)
-- ETL_STAGING_DB.usp_stg_txn_summary
--     @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL, @LookbackMonths INT = 12

-- T6  synapse/sql/staging/usp_stg_risk_factors.sql                         (C7 §2)
-- ETL_STAGING_DB.usp_stg_risk_factors
--     @RunId VARCHAR(64), @Env VARCHAR(10), @AsOfDate DATE = NULL

-- Error numbers (C3 §3): 50010 bad audit status | 50099 zero rows | 50100 < min rows | 50101 duplicate keys
