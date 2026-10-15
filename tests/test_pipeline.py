"""End-to-end tests for the local DuckDB engine and the Airflow DAG definition."""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "local" / "duckdb"))

import run_demo

# Columns read by the SAS programs (sas/0*.sas) from the staging layer.
SAS_STAGING_COLUMNS = {
    "etl_staging.stg_customer_360": {
        "customer_id", "age", "tenure_months", "customer_status", "segment_code", "state_code", "num_accounts",
        "num_active_accounts", "has_checking", "has_savings", "has_credit", "has_loan", "total_balance",
        "total_credit_limit", "credit_utilization_pct",
    },
    "etl_staging.stg_txn_summary": {
        "customer_id", "account_id", "account_type", "txn_count_total", "txn_count_debit", "txn_count_credit",
        "txn_count_fee", "amt_total_debit", "amt_total_credit", "amt_total_fees", "amt_avg_debit",
        "distinct_merchants", "top_merchant_category", "pct_atm", "pct_pos", "pct_web", "pct_mobile",
        "days_since_last_txn",
    },
    "etl_staging.stg_risk_factors": {
        "customer_id", "account_overdraft_cnt", "nsf_fee_total", "large_withdrawal_cnt", "balance_volatility",
        "credit_util_ratio", "payment_ontime_pct", "payment_late_cnt", "external_credit_score",
        "debit_velocity_7d", "debit_velocity_30d", "new_merchant_cnt_30d", "international_txn_cnt",
        "high_risk_merchant_cnt",
    },
}


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    path = tmp_path_factory.mktemp("duckdb") / "test.db"
    counts = run_demo.run_pipeline(path, n_customers=300, seed=7)
    con = run_demo.connect(path)
    yield con, counts
    con.close()


def test_all_product_tables_populated(db):
    _, counts = db
    assert set(counts) == set(run_demo.PRODUCT_TABLES)
    assert all(c > 0 for c in counts.values()), counts


def test_source_tables_populated(db):
    con, _ = db
    for table in run_demo.SOURCE_DDL:
        assert con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] > 0, table


@pytest.mark.parametrize("table", sorted(SAS_STAGING_COLUMNS))
def test_staging_columns_match_sas_inputs(db, table):
    con, _ = db
    schema, name = table.split(".")
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", [schema, name]
    ).fetchall()}
    missing = SAS_STAGING_COLUMNS[table] - cols
    assert not missing, f"{table} missing {missing}"


def test_customer_360_is_one_row_per_customer(db):
    con, _ = db
    total, distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT customer_id) FROM etl_staging.stg_customer_360"
    ).fetchone()
    assert total == distinct


def test_product_value_domains(db):
    con, _ = db
    assert con.execute(
        "SELECT COUNT(DISTINCT segment_name) FROM data_products.customer_segments"
    ).fetchone()[0] == 5
    bad_tiers = con.execute(
        "SELECT COUNT(*) FROM data_products.customer_risk_scores "
        "WHERE risk_tier NOT IN ('LOW','MODERATE','ELEVATED','HIGH','CRITICAL') "
        "OR probability_of_default NOT BETWEEN 0 AND 1 OR composite_risk_score NOT BETWEEN 0 AND 100"
    ).fetchone()[0]
    assert bad_tiers == 0
    assert con.execute(
        "SELECT COUNT(*) FROM data_products.transaction_analytics "
        "WHERE spend_percentile NOT BETWEEN 0 AND 100"
    ).fetchone()[0] == 0


def test_master_profile_keeps_every_base_customer(db):
    con, _ = db
    base = con.execute("SELECT COUNT(*) FROM etl_staging.stg_customer_360 WHERE customer_status = 'A'").fetchone()[0]
    master = con.execute("SELECT COUNT(*) FROM data_products.customer_master_profile").fetchone()[0]
    assert master == base


def test_validate_reports_zero_for_missing_products(tmp_path):
    with run_demo.connect(tmp_path / "empty.db") as con:
        assert run_demo.phase4_validate(con) == {t: 0 for t in run_demo.PRODUCT_TABLES}


def test_dag_structure():
    airflow_models = pytest.importorskip("airflow.models")
    bag = airflow_models.DagBag(dag_folder=str(REPO / "orchestration" / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags.get("retail_banking_pipeline")
    assert dag is not None and dag.schedule_interval is None
    expected = [
        "generate_source_data", "stg_customer_360", "stg_txn_summary", "stg_risk_factors",
        "customer_segments", "txn_analytics", "risk_scoring", "data_products", "validate_row_counts",
    ]
    assert [t.task_id for t in dag.topological_sort()] == expected
    for upstream, downstream in itertools.pairwise(expected):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}
