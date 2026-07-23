"""Tests for the Unity Catalog + Delta DDL bootstrap.

Runs ``ddl.create_delta_tables.run`` against a local Delta-enabled Spark session
and asserts that every schema and table is created with the expected columns and
types, and that ``transaction_analytics`` is partitioned by ``reporting_period``.
"""

from __future__ import annotations

import pytest

from ddl import create_delta_tables

# Expected column -> Spark simpleString type for every ported table, keyed by
# (schema_attr, table_name). schema_attr is the Config attribute holding the
# schema name so the assertions stay config-driven (Rule R5).
EXPECTED_SCHEMAS = {
    ("schema_core", "customers"): {
        "customer_id": "bigint", "first_name": "string", "last_name": "string",
        "date_of_birth": "date", "ssn_hash": "string", "email": "string",
        "phone_primary": "string", "customer_since": "date",
        "customer_status": "string", "segment_code": "string", "branch_id": "int",
        "created_ts": "timestamp", "updated_ts": "timestamp",
    },
    ("schema_core", "accounts"): {
        "account_id": "bigint", "customer_id": "bigint", "account_type": "string",
        "account_status": "string", "open_date": "date", "close_date": "date",
        "current_balance": "decimal(15,2)", "available_balance": "decimal(15,2)",
        "credit_limit": "decimal(15,2)", "interest_rate": "decimal(5,4)",
        "branch_id": "int", "created_ts": "timestamp", "updated_ts": "timestamp",
    },
    ("schema_core", "addresses"): {
        "address_id": "bigint", "customer_id": "bigint", "address_type": "string",
        "address_line_1": "string", "address_line_2": "string", "city": "string",
        "state_code": "string", "zip_code": "string", "country_code": "string",
        "is_primary": "string", "effective_date": "date", "expiration_date": "date",
        "created_ts": "timestamp", "updated_ts": "timestamp",
    },
    ("schema_txn", "transactions"): {
        "transaction_id": "bigint", "account_id": "bigint",
        "transaction_type_cd": "string", "transaction_date": "date",
        "transaction_ts": "timestamp", "amount": "decimal(15,2)",
        "running_balance": "decimal(15,2)", "merchant_name": "string",
        "merchant_category": "string", "channel_code": "string",
        "reference_num": "string", "status_code": "string", "created_ts": "timestamp",
    },
    ("schema_txn", "transaction_types"): {
        "transaction_type_cd": "string", "description": "string",
        "category": "string", "is_revenue": "string", "effective_date": "date",
        "expiration_date": "date",
    },
    ("schema_stg", "stg_customer_360"): {
        "customer_id": "bigint", "first_name": "string", "last_name": "string",
        "date_of_birth": "date", "age": "smallint", "customer_since": "date",
        "tenure_months": "int", "customer_status": "string", "segment_code": "string",
        "branch_id": "int", "primary_address": "string", "city": "string",
        "state_code": "string", "zip_code": "string", "num_accounts": "smallint",
        "num_active_accounts": "smallint", "has_checking": "string",
        "has_savings": "string", "has_credit": "string", "has_loan": "string",
        "total_balance": "decimal(18,2)", "total_credit_limit": "decimal(18,2)",
        "credit_utilization_pct": "decimal(5,2)", "load_ts": "timestamp",
    },
    ("schema_stg", "stg_txn_summary"): {
        "customer_id": "bigint", "account_id": "bigint", "account_type": "string",
        "summary_period_start": "date", "summary_period_end": "date",
        "txn_count_total": "int", "txn_count_debit": "int", "txn_count_credit": "int",
        "txn_count_fee": "int", "amt_total_debit": "decimal(18,2)",
        "amt_total_credit": "decimal(18,2)", "amt_total_fees": "decimal(18,2)",
        "amt_avg_debit": "decimal(15,2)", "amt_avg_credit": "decimal(15,2)",
        "amt_max_single_debit": "decimal(15,2)", "amt_max_single_credit": "decimal(15,2)",
        "distinct_merchants": "int", "top_merchant_category": "string",
        "pct_atm": "decimal(5,2)", "pct_pos": "decimal(5,2)", "pct_web": "decimal(5,2)",
        "pct_mobile": "decimal(5,2)", "days_since_last_txn": "int", "load_ts": "timestamp",
    },
    ("schema_stg", "stg_risk_factors"): {
        "customer_id": "bigint", "account_overdraft_cnt": "int",
        "nsf_fee_total": "decimal(15,2)", "large_withdrawal_cnt": "int",
        "large_withdrawal_amt": "decimal(18,2)", "avg_daily_balance_30d": "decimal(15,2)",
        "avg_daily_balance_90d": "decimal(15,2)", "balance_volatility": "decimal(10,4)",
        "credit_util_ratio": "decimal(5,4)", "payment_ontime_pct": "decimal(5,2)",
        "payment_late_cnt": "int", "months_since_last_late": "int",
        "external_credit_score": "int", "debit_velocity_7d": "decimal(15,2)",
        "debit_velocity_30d": "decimal(15,2)", "new_merchant_cnt_30d": "int",
        "international_txn_cnt": "int", "high_risk_merchant_cnt": "int",
        "load_ts": "timestamp",
    },
    ("schema_dp", "customer_segments"): {
        "customer_id": "bigint", "segment_name": "string", "segment_id": "smallint",
        "subsegment_id": "smallint", "lifetime_value_score": "decimal(10,2)",
        "engagement_score": "decimal(5,2)", "digital_adoption_score": "decimal(5,2)",
        "product_breadth_index": "decimal(5,2)", "tenure_group": "string",
        "age_group": "string", "balance_tier": "string", "channel_preference": "string",
        "cross_sell_flag": "string", "upsell_flag": "string",
        "retention_risk_flag": "string", "model_version": "string",
        "effective_date": "date", "load_ts": "timestamp",
    },
    ("schema_dp", "transaction_analytics"): {
        "customer_id": "bigint", "reporting_period": "string",
        "total_accounts": "smallint", "active_accounts": "smallint",
        "total_transactions": "int", "total_debit_amt": "decimal(18,2)",
        "total_credit_amt": "decimal(18,2)", "net_cash_flow": "decimal(18,2)",
        "avg_transaction_size": "decimal(15,2)", "monthly_spend_trend": "string",
        "spend_percentile": "decimal(5,2)", "top_spend_category": "string",
        "digital_txn_pct": "decimal(5,2)", "fee_income": "decimal(15,2)",
        "interest_income": "decimal(15,2)", "revenue_contribution": "decimal(15,2)",
        "anomaly_flag": "string", "model_version": "string", "effective_date": "date",
        "load_ts": "timestamp",
    },
    ("schema_dp", "customer_risk_scores"): {
        "customer_id": "bigint", "composite_risk_score": "decimal(6,2)",
        "risk_tier": "string", "probability_of_default": "decimal(7,6)",
        "credit_risk_component": "decimal(5,2)", "behaviour_risk_component": "decimal(5,2)",
        "velocity_risk_component": "decimal(5,2)", "bureau_score_component": "decimal(5,2)",
        "payment_history_component": "decimal(5,2)", "primary_risk_driver": "string",
        "secondary_risk_driver": "string", "score_delta_30d": "decimal(6,2)",
        "watch_list_flag": "string", "review_required_flag": "string",
        "model_version": "string", "effective_date": "date", "load_ts": "timestamp",
    },
    ("schema_dp", "customer_master_profile"): {
        "customer_id": "bigint", "full_name": "string", "age": "smallint",
        "state_code": "string", "customer_since": "date", "tenure_months": "int",
        "customer_status": "string", "segment_name": "string",
        "lifetime_value_score": "decimal(10,2)", "engagement_score": "decimal(5,2)",
        "total_accounts": "smallint", "active_accounts": "smallint",
        "total_balance": "decimal(18,2)", "total_credit_limit": "decimal(18,2)",
        "credit_utilization_pct": "decimal(5,2)", "monthly_transactions": "int",
        "monthly_spend": "decimal(18,2)", "net_cash_flow": "decimal(18,2)",
        "top_spend_category": "string", "digital_txn_pct": "decimal(5,2)",
        "composite_risk_score": "decimal(6,2)", "risk_tier": "string",
        "probability_of_default": "decimal(7,6)", "watch_list_flag": "string",
        "cross_sell_flag": "string", "upsell_flag": "string",
        "retention_risk_flag": "string", "model_version": "string",
        "effective_date": "date", "load_ts": "timestamp",
    },
}


@pytest.fixture(scope="module")
def bootstrapped(spark, cfg):
    """Run the bootstrap once for the module."""
    create_delta_tables.run(spark, cfg)
    return cfg


def test_all_schemas_created(spark, bootstrapped):
    cfg = bootstrapped
    existing = {row.namespace for row in spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}").collect()}
    for schema in cfg.schemas:
        assert schema in existing, f"missing schema {schema}"


@pytest.mark.parametrize("key", list(EXPECTED_SCHEMAS.keys()), ids=lambda k: f"{k[0]}.{k[1]}")
def test_table_columns_and_types(spark, cfg, bootstrapped, key):
    schema_attr, table_name = key
    fqn = cfg.table(getattr(cfg, schema_attr), table_name)
    actual = {f.name: f.dataType.simpleString() for f in spark.table(fqn).schema.fields}
    assert actual == EXPECTED_SCHEMAS[key]


def test_transaction_analytics_partitioned_by_reporting_period(spark, cfg, bootstrapped):
    fqn = cfg.table(cfg.schema_dp, "transaction_analytics")
    detail = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]
    assert detail["partitionColumns"] == ["reporting_period"]
    assert detail["format"] == "delta"


def test_bootstrap_is_idempotent(spark, cfg, bootstrapped):
    # Re-running must not raise and must not change the schema set.
    create_delta_tables.run(spark, cfg)
    fqn = cfg.table(cfg.schema_core, "customers")
    assert spark.table(fqn).count() == 0
