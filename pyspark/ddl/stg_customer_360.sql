-- =============================================================================
-- Delta DDL: etl_staging.stg_customer_360
-- Port of ddl/01_staging_tables.sql STG_CUSTOMER_360 (Teradata -> Delta / UC).
-- Populated by: pyspark/jobs/stg_customer_360.py
-- Table/catalog/schema names are templated with {catalog}/{schema_stg} by the
-- create_delta_tables.py bootstrap (no hardcoded names).
-- =============================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema_stg}.stg_customer_360 (
    customer_id             BIGINT          NOT NULL,
    first_name              STRING,
    last_name               STRING,
    date_of_birth           DATE,
    age                     SMALLINT,
    customer_since          DATE,
    tenure_months           INT,
    customer_status         STRING,
    segment_code            STRING,
    branch_id               INT,
    primary_address         STRING,
    city                    STRING,
    state_code              STRING,
    zip_code                STRING,
    num_accounts            SMALLINT,
    num_active_accounts     SMALLINT,
    has_checking            STRING,
    has_savings             STRING,
    has_credit              STRING,
    has_loan                STRING,
    total_balance           DECIMAL(18,2),
    total_credit_limit      DECIMAL(18,2),
    credit_utilization_pct  DECIMAL(5,2),
    load_ts                 TIMESTAMP
) USING delta;
