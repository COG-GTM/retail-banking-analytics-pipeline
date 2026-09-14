CREATE TABLE IF NOT EXISTS ${catalog}.core_banking.customers (
  customer_id BIGINT, first_name STRING, last_name STRING, date_of_birth DATE,
  ssn_hash STRING, email STRING, phone_primary STRING, customer_since DATE,
  customer_status STRING, segment_code STRING, branch_id INT, created_ts TIMESTAMP,
  updated_ts TIMESTAMP, _ingest_ts TIMESTAMP, _source_file STRING
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true',
  'delta.autoOptimize.autoCompact'='true'
);
CREATE TABLE IF NOT EXISTS ${catalog}.core_banking.accounts (
  account_id BIGINT, customer_id BIGINT, account_type STRING, account_status STRING,
  open_date DATE, close_date DATE, current_balance DECIMAL(15,2),
  available_balance DECIMAL(15,2), credit_limit DECIMAL(15,2), interest_rate DECIMAL(5,4),
  branch_id INT, created_ts TIMESTAMP, updated_ts TIMESTAMP, _ingest_ts TIMESTAMP,
  _source_file STRING
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true'
);
CREATE TABLE IF NOT EXISTS ${catalog}.core_banking.addresses (
  address_id BIGINT, customer_id BIGINT, address_type STRING, address_line_1 STRING,
  address_line_2 STRING, city STRING, state_code STRING, zip_code STRING, country_code STRING,
  is_primary STRING, effective_date DATE, expiration_date DATE, created_ts TIMESTAMP,
  updated_ts TIMESTAMP, _ingest_ts TIMESTAMP, _source_file STRING
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true'
);
CREATE TABLE IF NOT EXISTS ${catalog}.core_banking.customer_bureau_scores (
  customer_id BIGINT, external_credit_score INT, report_date DATE, _ingest_ts TIMESTAMP,
  _source_file STRING
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true'
);
CREATE TABLE IF NOT EXISTS ${catalog}.txn_processing.transactions (
  transaction_id BIGINT, account_id BIGINT, transaction_type_cd STRING, transaction_date DATE,
  transaction_ts TIMESTAMP, amount DECIMAL(15,2), running_balance DECIMAL(15,2),
  merchant_name STRING, merchant_category STRING, channel_code STRING, reference_num STRING,
  status_code STRING, created_ts TIMESTAMP, _ingest_ts TIMESTAMP, _source_file STRING
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true'
);
CREATE TABLE IF NOT EXISTS ${catalog}.txn_processing.transaction_types (
  transaction_type_cd STRING, description STRING, category STRING, is_revenue STRING,
  effective_date DATE, expiration_date DATE, _ingest_ts TIMESTAMP, _source_file STRING
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true'
);
