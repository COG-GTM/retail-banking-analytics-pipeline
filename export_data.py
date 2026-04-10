# /// script
# requires-python = ">=3.10"
# dependencies = ["faker>=28.0", "duckdb>=1.0", "pandas>=2.0", "scikit-learn>=1.3", "numpy>=1.24"]
# ///
"""
Generate realistic input and output data for the full pipeline:
  Source tables  ->  BTEQ staging  ->  SAS data products

Exports CSVs to data/01_source_tables, data/02_bteq_staging, data/03_sas_data_products

Usage:
  uv run export_data.py                   # 5,000 customers (default)
  uv run export_data.py --customers 10000 # custom count
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Import the DuckDB demo's pipeline engine
sys.path.insert(0, str(Path(__file__).resolve().parent / "local" / "duckdb"))
import run_demo  # type: ignore[import-untyped]

import duckdb

DATA_DIR = Path(__file__).resolve().parent / "data"


def export_table(con: duckdb.DuckDBPyConnection, schema_table: str, out_path: Path) -> int:
    """Export a DuckDB table to CSV, return row count."""
    df = con.execute(f"SELECT * FROM {schema_table}").fetchdf()
    df.to_csv(out_path, index=False)
    return len(df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=5000)
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 70)
    print("  Generating Full Pipeline Input/Output Data")
    print(f"  Customers: {args.customers:,}")
    print("=" * 70)

    dirs = {
        "source":   DATA_DIR / "01_source_tables",
        "staging":  DATA_DIR / "02_bteq_staging",
        "products": DATA_DIR / "03_sas_data_products",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")

    # ---- Phase 1: Create schemas, DDL, populate ----
    print("\n[Phase 1] Generating source data...")
    for schema in ("core_banking", "txn_processing", "etl_staging", "data_products"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    con.execute("CREATE OR REPLACE TABLE core_banking.customers (customer_id BIGINT PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, date_of_birth DATE, ssn_hash VARCHAR, email VARCHAR, phone_primary VARCHAR, customer_since DATE, customer_status VARCHAR, segment_code VARCHAR, branch_id INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE core_banking.accounts (account_id BIGINT PRIMARY KEY, customer_id BIGINT, account_type VARCHAR, account_status VARCHAR, open_date DATE, close_date DATE, current_balance DECIMAL(15,2), available_balance DECIMAL(15,2), credit_limit DECIMAL(15,2), interest_rate DECIMAL(5,4), branch_id INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE core_banking.addresses (address_id BIGINT PRIMARY KEY, customer_id BIGINT, address_type VARCHAR, address_line_1 VARCHAR, address_line_2 VARCHAR, city VARCHAR, state_code VARCHAR, zip_code VARCHAR, country_code VARCHAR, is_primary VARCHAR, effective_date DATE, expiration_date DATE, created_ts TIMESTAMP, updated_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE txn_processing.transactions (transaction_id BIGINT PRIMARY KEY, account_id BIGINT, transaction_type_cd VARCHAR, transaction_date DATE, transaction_ts TIMESTAMP, amount DECIMAL(15,2), running_balance DECIMAL(15,2), merchant_name VARCHAR, merchant_category VARCHAR, channel_code VARCHAR, reference_num VARCHAR, status_code VARCHAR, created_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE txn_processing.transaction_types (transaction_type_cd VARCHAR PRIMARY KEY, description VARCHAR, category VARCHAR, is_revenue VARCHAR, effective_date DATE, expiration_date DATE)")
    con.execute("CREATE OR REPLACE TABLE core_banking.customer_bureau_scores (customer_id BIGINT, external_credit_score INTEGER, report_date DATE)")

    run_demo._builtin_populate_sources(con, n_customers=args.customers)

    source_exports = [
        ("customers",              "core_banking.customers"),
        ("accounts",               "core_banking.accounts"),
        ("addresses",              "core_banking.addresses"),
        ("transaction_types",      "txn_processing.transaction_types"),
        ("transactions",           "txn_processing.transactions"),
        ("customer_bureau_scores", "core_banking.customer_bureau_scores"),
    ]
    for fname, tbl in source_exports:
        n = export_table(con, tbl, dirs["source"] / f"{fname}.csv")
        print(f"  {fname}.csv: {n:>10,} rows")

    # ---- Phase 2: BTEQ staging transformations ----
    print("\n[Phase 2] Running BTEQ transformations...")
    run_demo.phase2_bteq_transforms(con)

    staging_exports = [
        ("stg_customer_360", "etl_staging.stg_customer_360"),
        ("stg_txn_summary",  "etl_staging.stg_txn_summary"),
        ("stg_risk_factors", "etl_staging.stg_risk_factors"),
    ]
    for fname, tbl in staging_exports:
        n = export_table(con, tbl, dirs["staging"] / f"{fname}.csv")
        print(f"  {fname}.csv: {n:>10,} rows")

    # ---- Phase 3: SAS data products ----
    print("\n[Phase 3] Running SAS analytics (Python/scikit-learn)...")
    run_demo.phase3_python_analytics(con)

    product_exports = [
        ("customer_segments",       "data_products.customer_segments"),
        ("transaction_analytics",   "data_products.transaction_analytics"),
        ("customer_risk_scores",    "data_products.customer_risk_scores"),
        ("customer_master_profile", "data_products.customer_master_profile"),
    ]
    for fname, tbl in product_exports:
        n = export_table(con, tbl, dirs["products"] / f"{fname}.csv")
        print(f"  {fname}.csv: {n:>10,} rows")

    con.close()

    elapsed = time.time() - t0
    total_files = len(source_exports) + len(staging_exports) + len(product_exports)
    print(f"\n{'=' * 70}")
    print(f"  {total_files} CSV files written to {DATA_DIR}/")
    print(f"  Completed in {elapsed:.1f}s")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
