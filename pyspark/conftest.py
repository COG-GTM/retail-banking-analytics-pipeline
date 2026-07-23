"""Shared pytest fixtures: a Delta-enabled local SparkSession seeded from the
committed ``data/`` CSVs.

Source tables are registered under the local ``spark_catalog`` so jobs can read
them via ``Config.table(...)`` exactly as they would from Unity Catalog. The
fixtures pin ``run_date`` to the date the committed CSV fixtures were generated so
wall-clock-derived fields (period bounds, ``days_since_last_txn``) are reproducible.
"""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
SOURCE_DIR = DATA_DIR / "01_source_tables"
STAGING_DIR = DATA_DIR / "02_bteq_staging"

# The committed fixtures were generated as-of this date (run_date = 2026-04-10;
# a 12-month lookback yields 2025-04-10 .. 2026-04-10).
FIXTURE_RUN_DATE = _dt.date(2026, 4, 10)

_LOCAL_CATALOG = "spark_catalog"

# Explicit read schemas for the source CSVs (only the columns the jobs consume
# need exact types; the rest keep their string form).
_TRANSACTIONS_SCHEMA = (
    "transaction_id BIGINT, account_id BIGINT, transaction_type_cd STRING, "
    "transaction_date DATE, transaction_ts TIMESTAMP, amount DOUBLE, "
    "running_balance DOUBLE, merchant_name STRING, merchant_category STRING, "
    "channel_code STRING, reference_num STRING, status_code STRING, created_ts TIMESTAMP"
)
_ACCOUNTS_SCHEMA = (
    "account_id BIGINT, customer_id BIGINT, account_type STRING, account_status STRING, "
    "open_date DATE, close_date DATE, current_balance DOUBLE, available_balance DOUBLE, "
    "credit_limit DOUBLE, interest_rate DOUBLE, branch_id INT, created_ts TIMESTAMP, "
    "updated_ts TIMESTAMP"
)
_TXN_TYPES_SCHEMA = (
    "transaction_type_cd STRING, description STRING, category STRING, is_revenue STRING, "
    "effective_date DATE, expiration_date DATE"
)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    os.environ["SPARK_WAREHOUSE_DIR"] = str(warehouse)

    from common.spark import get_spark

    session = get_spark("retail_banking_tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def config():
    from common.config import Config

    return Config(catalog=_LOCAL_CATALOG, run_date=FIXTURE_RUN_DATE)


def _register_csv(spark, fqn: str, path: Path, schema: str) -> None:
    schema_name = fqn.split(".")[1]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_LOCAL_CATALOG}.{schema_name}")
    df = spark.read.option("header", True).schema(schema).csv(str(path))
    df.write.format("delta").mode("overwrite").saveAsTable(fqn)


@pytest.fixture(scope="session")
def seeded_sources(spark, config):
    """Seed source tables into ``spark_catalog`` from the committed source CSVs."""
    _register_csv(
        spark,
        config.table(config.schema_txn, "transactions"),
        SOURCE_DIR / "transactions.csv",
        _TRANSACTIONS_SCHEMA,
    )
    _register_csv(
        spark,
        config.table(config.schema_core, "accounts"),
        SOURCE_DIR / "accounts.csv",
        _ACCOUNTS_SCHEMA,
    )
    _register_csv(
        spark,
        config.table(config.schema_txn, "transaction_types"),
        SOURCE_DIR / "transaction_types.csv",
        _TXN_TYPES_SCHEMA,
    )
    return config


@pytest.fixture(scope="session")
def expected_stg_txn_summary(spark):
    """The committed BTEQ-staging fixture, for cross-checking parity where practical."""
    schema = (
        "customer_id BIGINT, account_id BIGINT, account_type STRING, "
        "summary_period_start DATE, summary_period_end DATE, txn_count_total INT, "
        "txn_count_debit DOUBLE, txn_count_credit DOUBLE, txn_count_fee DOUBLE, "
        "amt_total_debit DOUBLE, amt_total_credit DOUBLE, amt_total_fees DOUBLE, "
        "amt_avg_debit DOUBLE, amt_avg_credit DOUBLE, amt_max_single_debit DOUBLE, "
        "amt_max_single_credit DOUBLE, distinct_merchants INT, top_merchant_category STRING, "
        "pct_atm DOUBLE, pct_pos DOUBLE, pct_web DOUBLE, pct_mobile DOUBLE, "
        "days_since_last_txn INT, load_ts STRING"
    )
    return (
        spark.read.option("header", True)
        .schema(schema)
        .csv(str(STAGING_DIR / "stg_txn_summary.csv"))
    )
