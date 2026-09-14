CREATE TABLE IF NOT EXISTS ${catalog}.etl_staging.etl_run_log (
  job_name STRING, step_name STRING, status STRING, message STRING, row_count BIGINT,
  run_date DATE, log_ts TIMESTAMP
) USING DELTA TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true'
);
