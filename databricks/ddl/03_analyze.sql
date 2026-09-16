-- =============================================================================
-- Replaces Teradata COLLECT STATISTICS with Delta ANALYZE TABLE.
-- =============================================================================

ANALYZE TABLE ${catalog}.etl_staging.stg_customer_360
    COMPUTE STATISTICS FOR COLUMNS customer_id;
ANALYZE TABLE ${catalog}.etl_staging.stg_txn_summary
    COMPUTE STATISTICS FOR COLUMNS customer_id, account_id;
ANALYZE TABLE ${catalog}.etl_staging.stg_risk_factors
    COMPUTE STATISTICS FOR COLUMNS customer_id;

ANALYZE TABLE ${catalog}.data_products.customer_segments
    COMPUTE STATISTICS FOR COLUMNS customer_id, segment_name;
ANALYZE TABLE ${catalog}.data_products.transaction_analytics
    COMPUTE STATISTICS FOR COLUMNS customer_id, reporting_period;
ANALYZE TABLE ${catalog}.data_products.customer_risk_scores
    COMPUTE STATISTICS FOR COLUMNS customer_id, risk_tier;
ANALYZE TABLE ${catalog}.data_products.customer_master_profile
    COMPUTE STATISTICS FOR COLUMNS customer_id;
