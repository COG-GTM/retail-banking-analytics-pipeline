"""Unit tests for staging.stg_txn_summary."""
from datetime import date, datetime

from conftest import make_df

from staging.stg_txn_summary import OUTPUT_COLUMNS, transform

AS_OF = date(2026, 4, 10)
LOOKBACK = 12  # period: 2025-04-10 .. 2026-04-10


def _run(spark):
    accounts = [
        {"account_id": 100, "customer_id": 1, "account_type": "CHECKING",
         "account_status": "O", "open_date": date(2020, 1, 1)},
    ]
    transaction_types = [
        {"transaction_type_cd": "PUR", "description": "Purchase", "category": "DEBIT"},
        {"transaction_type_cd": "DEP", "description": "Deposit", "category": "CREDIT"},
        {"transaction_type_cd": "FEE", "description": "Account Fee", "category": "FEE"},
    ]
    txns = [
        # Debits (ATM channel), posted, in period.
        {"transaction_id": 1, "account_id": 100, "transaction_type_cd": "PUR",
         "transaction_date": date(2026, 4, 8), "transaction_ts": datetime(2026, 4, 8, 9),
         "amount": -100.00, "merchant_name": "Shop A", "merchant_category": "GROCERY",
         "channel_code": "ATM", "status_code": "P"},
        {"transaction_id": 2, "account_id": 100, "transaction_type_cd": "PUR",
         "transaction_date": date(2026, 4, 1), "transaction_ts": datetime(2026, 4, 1, 9),
         "amount": -300.00, "merchant_name": "Shop B", "merchant_category": "GROCERY",
         "channel_code": "POS", "status_code": "P"},
        # Credit (deposit).
        {"transaction_id": 3, "account_id": 100, "transaction_type_cd": "DEP",
         "transaction_date": date(2026, 3, 1), "transaction_ts": datetime(2026, 3, 1, 9),
         "amount": 500.00, "merchant_name": "Employer", "merchant_category": "PAYROLL",
         "channel_code": "WEB", "status_code": "P"},
        # Fee.
        {"transaction_id": 4, "account_id": 100, "transaction_type_cd": "FEE",
         "transaction_date": date(2026, 2, 1), "transaction_ts": datetime(2026, 2, 1, 9),
         "amount": -25.00, "merchant_name": None, "merchant_category": None,
         "channel_code": "WEB", "status_code": "P"},
        # Non-posted (H) -> excluded.
        {"transaction_id": 5, "account_id": 100, "transaction_type_cd": "PUR",
         "transaction_date": date(2026, 4, 9), "transaction_ts": datetime(2026, 4, 9, 9),
         "amount": -999.00, "merchant_name": "Ghost", "merchant_category": "GROCERY",
         "channel_code": "ATM", "status_code": "H"},
        # Out of period (older than lookback) -> excluded.
        {"transaction_id": 6, "account_id": 100, "transaction_type_cd": "PUR",
         "transaction_date": date(2024, 1, 1), "transaction_ts": datetime(2024, 1, 1, 9),
         "amount": -50.00, "merchant_name": "OldShop", "merchant_category": "RETAIL",
         "channel_code": "POS", "status_code": "P"},
    ]
    return transform(
        make_df(spark, "transactions", txns),
        make_df(spark, "transaction_types", transaction_types),
        make_df(spark, "accounts", accounts),
        AS_OF, LOOKBACK,
    )


def test_output_schema(spark):
    assert _run(spark).columns == OUTPUT_COLUMNS


def test_aggregations(spark):
    result = _run(spark)
    assert result.count() == 1  # one account
    row = result.collect()[0]

    # Only 4 posted, in-period txns (excludes the H and the 2024 rows).
    assert row["txn_count_total"] == 4
    assert row["txn_count_debit"] == 2
    assert row["txn_count_credit"] == 1
    assert row["txn_count_fee"] == 1

    # abs() applied to debits/fees; credit kept signed.
    assert float(row["amt_total_debit"]) == 400.00
    assert float(row["amt_total_credit"]) == 500.00
    assert float(row["amt_total_fees"]) == 25.00
    assert float(row["amt_max_single_debit"]) == 300.00

    # distinct merchants counts non-null names only (Shop A, Shop B, Employer).
    assert row["distinct_merchants"] == 3
    # Top category by total abs spend across all posted txns (incl. credits):
    # PAYROLL = 500 beats GROCERY = 100 + 300 = 400.
    assert row["top_merchant_category"] == "PAYROLL"

    # Channel mix over 4 txns: ATM 1/4 = 25.00%, POS 1/4 = 25.00%, WEB 2/4 = 50.00%.
    assert float(row["pct_atm"]) == 25.00
    assert float(row["pct_pos"]) == 25.00
    assert float(row["pct_web"]) == 50.00
    assert float(row["pct_mobile"]) == 0.00

    # Period bounds and recency.
    assert row["summary_period_start"] == date(2025, 4, 10)
    assert row["summary_period_end"] == AS_OF
    assert row["days_since_last_txn"] == 2  # last posted txn on 2026-04-08
