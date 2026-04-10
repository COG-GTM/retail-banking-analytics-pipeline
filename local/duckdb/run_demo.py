# /// script
# requires-python = ">=3.10"
# dependencies = ["faker>=28.0", "duckdb>=1.0", "pandas>=2.0", "scikit-learn>=1.3"]
# ///
"""
DuckDB + Python Demo — Zero-install replacement for Teradata BTEQ + SAS pipeline.

Run with:  uv run local/duckdb/run_demo.py

This single file reproduces the full retail-banking analytics pipeline:
  Phase 1  Create DuckDB schemas and load synthetic data  (replaces Teradata DDL)
  Phase 2  Run BTEQ-equivalent SQL transformations         (replaces 3 BTEQ scripts)
  Phase 3  Run Python/scikit-learn analytics               (replaces 4 SAS programs)
  Phase 4  Print summary report to stdout
"""
from __future__ import annotations

import sys
import time
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Try to import the shared data generator; fall back to built-in generator
# ---------------------------------------------------------------------------
_GENERATE_DATA_AVAILABLE = False
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from generate_data import populate_sources  # type: ignore[import-untyped]
    _GENERATE_DATA_AVAILABLE = True
except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# BUILT-IN SYNTHETIC DATA GENERATOR  (used when generate_data.py is absent)
# ═══════════════════════════════════════════════════════════════════════════════

def _builtin_populate_sources(con: duckdb.DuckDBPyConnection, *, n_customers: int = 2000) -> None:
    """Generate realistic banking data directly into DuckDB using vectorised ops."""
    from faker import Faker
    fake = Faker()
    Faker.seed(42)
    rng = np.random.default_rng(42)

    today = date.today()
    _d = lambda days_ago: today - timedelta(days=days_ago)   # noqa: E731

    # --- Customers (vectorised) -----------------------------------------------
    first_names = [fake.first_name() for _ in range(n_customers)]
    last_names  = [fake.last_name()  for _ in range(n_customers)]
    dob_offsets = rng.integers(18 * 365, 85 * 365, size=n_customers)
    since_offsets = rng.integers(30, 20 * 365, size=n_customers)
    cust_df = pd.DataFrame({
        "customer_id":    range(1, n_customers + 1),
        "first_name":     first_names,
        "last_name":      last_names,
        "date_of_birth":  [_d(int(d)) for d in dob_offsets],
        "ssn_hash":       [f"sha256_{i:06d}" for i in range(n_customers)],
        "email":          [f"user{i}@example.com" for i in range(n_customers)],
        "phone_primary":  [f"555-{rng.integers(100,999)}-{rng.integers(1000,9999)}" for _ in range(n_customers)],
        "customer_since": [_d(int(d)) for d in since_offsets],
        "customer_status": rng.choice(["A"] * 80 + ["I"] * 15 + ["C"] * 5, size=n_customers),
        "segment_code":    rng.choice(["PREMIER", "PREFERRED", "STANDARD", "DIGITAL", "BASIC"], size=n_customers),
        "branch_id":       rng.integers(100, 500, size=n_customers).astype(int),
        "created_ts":      datetime.now(), "updated_ts": datetime.now(),
    })
    con.register("_cust", cust_df)
    con.execute("INSERT INTO core_banking.customers SELECT * FROM _cust")
    con.unregister("_cust")

    # --- Addresses (2 per customer) -------------------------------------------
    n_addr = n_customers * 2
    eff_offsets = rng.integers(30, 5 * 365, size=n_addr)
    addr_df = pd.DataFrame({
        "address_id":    range(1, n_addr + 1),
        "customer_id":   [c for c in range(1, n_customers + 1) for _ in range(2)],
        "address_type":  ["HOME", "MAIL"] * n_customers,
        "address_line_1": [f"{rng.integers(100,9999)} {fake.street_name()}" for _ in range(n_addr)],
        "address_line_2": [f"Apt {rng.integers(1,300)}" if rng.random() > 0.6 else None for _ in range(n_addr)],
        "city":          [fake.city() for _ in range(n_addr)],
        "state_code":    rng.choice(["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI"], size=n_addr),
        "zip_code":      [f"{rng.integers(10000,99999)}" for _ in range(n_addr)],
        "country_code":  "US", "is_primary": ["Y", "N"] * n_customers,
        "effective_date": [_d(int(d)) for d in eff_offsets],
        "expiration_date": pd.NaT,
        "created_ts": datetime.now(), "updated_ts": datetime.now(),
    })
    con.register("_addr", addr_df)
    con.execute("INSERT INTO core_banking.addresses SELECT * FROM _addr")
    con.unregister("_addr")

    # --- Accounts (1-4 per customer) ------------------------------------------
    acct_types_all = ["CHECKING", "SAVINGS", "CREDIT", "LOAN"]
    acct_rows_list: list[dict] = []
    acct_id = 0
    cid_accts: dict[int, list[int]] = {}
    for cid in range(1, n_customers + 1):
        n_accts = int(rng.choice([1, 2, 3, 4], p=[0.15, 0.40, 0.30, 0.15]))
        chosen = list(rng.choice(acct_types_all, size=n_accts, replace=False))
        for atype in chosen:
            acct_id += 1
            bal = round(float(rng.lognormal(8, 2)), 2)
            acct_rows_list.append({
                "account_id": acct_id, "customer_id": cid,
                "account_type": atype,
                "account_status": str(rng.choice(["O"] * 85 + ["C"] * 10 + ["F"] * 5)),
                "open_date": _d(int(rng.integers(60, 10 * 365))),
                "close_date": None,
                "current_balance": bal, "available_balance": round(bal * 0.9, 2),
                "credit_limit": round(bal * float(rng.uniform(1.5, 5)), 2) if atype == "CREDIT" else None,
                "interest_rate": round(float(rng.uniform(0.01, 0.25)), 4) if atype in ("CREDIT", "LOAN") else round(float(rng.uniform(0.001, 0.05)), 4),
                "branch_id": int(rng.integers(100, 500)),
                "created_ts": datetime.now(), "updated_ts": datetime.now(),
            })
            cid_accts.setdefault(cid, []).append(acct_id)
    acct_df = pd.DataFrame(acct_rows_list)
    con.register("_acct", acct_df)
    con.execute("INSERT INTO core_banking.accounts SELECT * FROM _acct")
    con.unregister("_acct")

    # --- Transaction types (reference table) ----------------------------------
    tt_df = pd.DataFrame([
        {"transaction_type_cd": "PUR", "description": "Purchase",    "category": "DEBIT",  "is_revenue": "N", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "WDR", "description": "Withdrawal",  "category": "DEBIT",  "is_revenue": "N", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "DEP", "description": "Deposit",     "category": "CREDIT", "is_revenue": "N", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "PMT", "description": "Payment",     "category": "CREDIT", "is_revenue": "N", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "FEE", "description": "Account Fee", "category": "FEE",    "is_revenue": "Y", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "NSF", "description": "NSF Fee",     "category": "FEE",    "is_revenue": "Y", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "INT", "description": "Interest",    "category": "CREDIT", "is_revenue": "Y", "effective_date": "2020-01-01", "expiration_date": None},
        {"transaction_type_cd": "TRF", "description": "Transfer",    "category": "DEBIT",  "is_revenue": "N", "effective_date": "2020-01-01", "expiration_date": None},
    ])
    con.register("_tt", tt_df)
    con.execute("INSERT INTO txn_processing.transaction_types SELECT * FROM _tt")
    con.unregister("_tt")

    # --- Transactions (bulk-vectorised) ---------------------------------------
    channels = ["ATM", "POS", "WEB", "MOB", "ACH"]
    categories = ["GROCERY", "RESTAURANT", "RETAIL", "UTILITIES", "TRAVEL",
                   "HEALTHCARE", "ENTERTAINMENT", "GAS_STATION", "GAMBLING",
                   "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE"]
    txn_codes = ["PUR", "WDR", "DEP", "PMT", "FEE", "NSF", "INT", "TRF"]
    txn_probs = [0.35, 0.10, 0.20, 0.15, 0.05, 0.02, 0.03, 0.10]
    debit_codes = {"PUR", "WDR", "FEE", "NSF", "TRF"}
    # Pre-generate a merchant pool to avoid per-row Faker calls
    merchant_pool = [fake.company().replace("'", "") for _ in range(200)]

    # Build account→n_txns mapping
    acc_ids_expanded: list[int] = []
    for accts in cid_accts.values():
        for acc_id in accts:
            n_txns = int(rng.integers(10, 120))
            acc_ids_expanded.extend([acc_id] * n_txns)
    total_txns = len(acc_ids_expanded)

    # Vectorised generation
    txn_df = pd.DataFrame({
        "transaction_id":    range(1, total_txns + 1),
        "account_id":        acc_ids_expanded,
        "transaction_type_cd": rng.choice(txn_codes, size=total_txns, p=txn_probs),
        "transaction_date":  [_d(int(d)) for d in rng.integers(0, 365, size=total_txns)],
        "amount":            np.round(rng.lognormal(4, 1.5, size=total_txns), 2),
        "merchant_idx":      rng.integers(0, len(merchant_pool), size=total_txns),
        "merchant_cat_idx":  rng.integers(0, len(categories), size=total_txns),
        "channel_code":      rng.choice(channels, size=total_txns),
        "reference_num":     [f"REF{i:08d}" for i in range(total_txns)],
        "status_code":       rng.choice(["P", "P", "P", "P", "R", "H"], size=total_txns),
        "created_ts":        datetime.now(),
    })
    # Negate debit amounts
    is_debit = txn_df["transaction_type_cd"].isin(debit_codes)
    txn_df.loc[is_debit, "amount"] = -txn_df.loc[is_debit, "amount"]
    # Running balance (cumulative per account)
    txn_df["running_balance"] = txn_df.groupby("account_id")["amount"].cumsum() + rng.uniform(500, 50000, size=total_txns)
    txn_df["running_balance"] = txn_df["running_balance"].round(2)
    # Merchant name only for purchases
    is_purchase = txn_df["transaction_type_cd"] == "PUR"
    txn_df["merchant_name"] = None
    txn_df.loc[is_purchase, "merchant_name"] = txn_df.loc[is_purchase, "merchant_idx"].map(lambda i: merchant_pool[i])
    txn_df["merchant_category"] = None
    txn_df.loc[is_purchase, "merchant_category"] = txn_df.loc[is_purchase, "merchant_cat_idx"].map(lambda i: categories[i])
    # Timestamp from date
    txn_df["transaction_ts"] = pd.to_datetime(txn_df["transaction_date"]) + pd.to_timedelta(rng.integers(0, 86400, size=total_txns), unit="s")

    txn_final = txn_df[["transaction_id", "account_id", "transaction_type_cd",
                         "transaction_date", "transaction_ts", "amount",
                         "running_balance", "merchant_name", "merchant_category",
                         "channel_code", "reference_num", "status_code", "created_ts"]]
    con.register("_txn", txn_final)
    con.execute("INSERT INTO txn_processing.transactions SELECT * FROM _txn")
    con.unregister("_txn")

    # --- Customer bureau scores -----------------------------------------------
    bureau_df = pd.DataFrame({
        "customer_id": range(1, n_customers + 1),
        "external_credit_score": np.clip(rng.normal(710, 80, size=n_customers).astype(int), 300, 850),
        "report_date": [_d(int(d)) for d in rng.integers(0, 90, size=n_customers)],
    })
    con.register("_bureau", bureau_df)
    con.execute("INSERT INTO core_banking.customer_bureau_scores SELECT * FROM _bureau")
    con.unregister("_bureau")

    print(f"    Loaded {n_customers:,} customers, {acct_id:,} accounts, "
          f"{total_txns:,} transactions")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — CREATE DUCKDB SCHEMAS & LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════════

def phase1_create_and_load(con: duckdb.DuckDBPyConnection) -> None:
    """Create schemas, source tables, and populate with synthetic data."""
    print("\n" + "=" * 72)
    print("PHASE 1: Create DuckDB database and load synthetic data")
    print("=" * 72)

    # Schemas mirror the Teradata databases
    for schema in ("core_banking", "txn_processing", "etl_staging", "data_products"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print("    Created schemas: core_banking, txn_processing, etl_staging, data_products")

    # Source tables (translated from ddl/00_source_tables.sql)
    con.execute("""
        CREATE OR REPLACE TABLE core_banking.customers (
            customer_id       BIGINT PRIMARY KEY,
            first_name        VARCHAR, last_name VARCHAR,
            date_of_birth     DATE, ssn_hash VARCHAR, email VARCHAR,
            phone_primary     VARCHAR, customer_since DATE,
            customer_status   VARCHAR, segment_code VARCHAR,
            branch_id         INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE core_banking.accounts (
            account_id      BIGINT PRIMARY KEY,
            customer_id     BIGINT, account_type VARCHAR,
            account_status  VARCHAR, open_date DATE, close_date DATE,
            current_balance DECIMAL(15,2), available_balance DECIMAL(15,2),
            credit_limit    DECIMAL(15,2), interest_rate DECIMAL(5,4),
            branch_id       INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE core_banking.addresses (
            address_id      BIGINT PRIMARY KEY,
            customer_id     BIGINT, address_type VARCHAR,
            address_line_1  VARCHAR, address_line_2 VARCHAR,
            city VARCHAR, state_code VARCHAR, zip_code VARCHAR,
            country_code    VARCHAR, is_primary VARCHAR,
            effective_date  DATE, expiration_date DATE,
            created_ts TIMESTAMP, updated_ts TIMESTAMP)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE txn_processing.transactions (
            transaction_id    BIGINT PRIMARY KEY,
            account_id        BIGINT, transaction_type_cd VARCHAR,
            transaction_date  DATE, transaction_ts TIMESTAMP,
            amount            DECIMAL(15,2), running_balance DECIMAL(15,2),
            merchant_name     VARCHAR, merchant_category VARCHAR,
            channel_code      VARCHAR, reference_num VARCHAR,
            status_code       VARCHAR, created_ts TIMESTAMP)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE txn_processing.transaction_types (
            transaction_type_cd VARCHAR PRIMARY KEY,
            description VARCHAR, category VARCHAR,
            is_revenue  VARCHAR, effective_date DATE, expiration_date DATE)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE core_banking.customer_bureau_scores (
            customer_id          BIGINT,
            external_credit_score INTEGER,
            report_date          DATE)
    """)
    print("    Created source tables")

    # Populate
    if _GENERATE_DATA_AVAILABLE:
        print("    Using shared generate_data.populate_sources …")
        populate_sources(con)
    else:
        print("    Using built-in synthetic data generator …")
        _builtin_populate_sources(con)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — BTEQ TRANSFORMATIONS AS DUCKDB SQL
# ═══════════════════════════════════════════════════════════════════════════════

def phase2_bteq_transforms(con: duckdb.DuckDBPyConnection) -> None:
    """Translate the three BTEQ scripts into DuckDB SQL.

    DuckDB supports QUALIFY, ROW_NUMBER(), window functions natively, so the
    SQL is almost identical to the original Teradata BTEQ.
    """
    print("\n" + "=" * 72)
    print("PHASE 2: Run BTEQ-equivalent SQL transformations in DuckDB")
    print("=" * 72)

    # -------------------------------------------------------------------
    # 2a  STG_CUSTOMER_360  (01_stg_customer_360.bteq)
    # -------------------------------------------------------------------
    print("    [2a] Building etl_staging.stg_customer_360 …")
    con.execute("""
    CREATE OR REPLACE TABLE etl_staging.stg_customer_360 AS
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        CAST(datediff('year', c.date_of_birth, current_date) AS SMALLINT) AS age,
        c.customer_since,
        CAST(datediff('month', c.customer_since, current_date) AS INTEGER) AS tenure_months,
        c.customer_status,
        c.segment_code,
        c.branch_id,
        a.address_line_1 || COALESCE(', ' || a.address_line_2, '') AS primary_address,
        a.city,
        a.state_code,
        a.zip_code,
        acct_agg.num_accounts,
        acct_agg.num_active_accounts,
        acct_agg.has_checking,
        acct_agg.has_savings,
        acct_agg.has_credit,
        acct_agg.has_loan,
        acct_agg.total_balance,
        acct_agg.total_credit_limit,
        CASE WHEN acct_agg.total_credit_limit > 0
             THEN CAST(acct_agg.credit_balance / acct_agg.total_credit_limit * 100
                        AS DECIMAL(5,2))
             ELSE 0.00 END AS credit_utilization_pct,
        current_timestamp AS load_ts
    FROM core_banking.customers c
    LEFT JOIN (
        SELECT customer_id, address_line_1, address_line_2, city, state_code, zip_code
        FROM core_banking.addresses
        WHERE address_type = 'HOME'
          AND (expiration_date IS NULL OR expiration_date > current_date)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY effective_date DESC) = 1
    ) a ON c.customer_id = a.customer_id
    LEFT JOIN (
        SELECT
            customer_id,
            COUNT(*)                                                          AS num_accounts,
            SUM(CASE WHEN account_status='O' THEN 1 ELSE 0 END)             AS num_active_accounts,
            MAX(CASE WHEN account_type='CHECKING' THEN 'Y' ELSE 'N' END)    AS has_checking,
            MAX(CASE WHEN account_type='SAVINGS'  THEN 'Y' ELSE 'N' END)    AS has_savings,
            MAX(CASE WHEN account_type='CREDIT'   THEN 'Y' ELSE 'N' END)    AS has_credit,
            MAX(CASE WHEN account_type='LOAN'     THEN 'Y' ELSE 'N' END)    AS has_loan,
            SUM(COALESCE(current_balance, 0))                                 AS total_balance,
            SUM(CASE WHEN account_type='CREDIT' THEN COALESCE(credit_limit,0)
                 ELSE 0 END)                                                  AS total_credit_limit,
            SUM(CASE WHEN account_type='CREDIT' THEN COALESCE(current_balance,0)
                 ELSE 0 END)                                                  AS credit_balance
        FROM core_banking.accounts GROUP BY customer_id
    ) acct_agg ON c.customer_id = acct_agg.customer_id
    WHERE c.customer_status IN ('A', 'I')
    """)
    cnt = con.execute("SELECT count(*) FROM etl_staging.stg_customer_360").fetchone()[0]
    print(f"        → {cnt:,} rows")

    # -------------------------------------------------------------------
    # 2b  STG_TXN_SUMMARY  (02_stg_txn_summary.bteq)
    # -------------------------------------------------------------------
    print("    [2b] Building etl_staging.stg_txn_summary …")
    lookback_months = 12
    con.execute(f"""
    CREATE OR REPLACE TABLE etl_staging.stg_txn_summary AS
    WITH run_params AS (
        SELECT
            current_date - INTERVAL '{lookback_months}' MONTH AS period_start,
            current_date                                       AS period_end
    ),
    top_cat AS (
        SELECT account_id, merchant_category
        FROM (
            SELECT
                t2.account_id,
                t2.merchant_category,
                SUM(ABS(t2.amount)) AS cat_spend,
                ROW_NUMBER() OVER (
                    PARTITION BY t2.account_id ORDER BY SUM(ABS(t2.amount)) DESC
                ) AS rn
            FROM txn_processing.transactions t2, run_params rp2
            WHERE t2.transaction_date BETWEEN rp2.period_start AND rp2.period_end
              AND t2.status_code = 'P'
              AND t2.merchant_category IS NOT NULL
            GROUP BY t2.account_id, t2.merchant_category
        ) sub WHERE rn = 1
    )
    SELECT
        acct.customer_id,
        acct.account_id,
        acct.account_type,
        rp.period_start                                              AS summary_period_start,
        rp.period_end                                                AS summary_period_end,
        COUNT(*)                                                     AS txn_count_total,
        SUM(CASE WHEN tt.category='DEBIT'  THEN 1 ELSE 0 END)      AS txn_count_debit,
        SUM(CASE WHEN tt.category='CREDIT' THEN 1 ELSE 0 END)      AS txn_count_credit,
        SUM(CASE WHEN tt.category='FEE'    THEN 1 ELSE 0 END)      AS txn_count_fee,
        SUM(CASE WHEN tt.category='DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_total_debit,
        SUM(CASE WHEN tt.category='CREDIT' THEN t.amount      ELSE 0 END) AS amt_total_credit,
        SUM(CASE WHEN tt.category='FEE'    THEN ABS(t.amount) ELSE 0 END) AS amt_total_fees,
        AVG(CASE WHEN tt.category='DEBIT'  THEN ABS(t.amount) END)        AS amt_avg_debit,
        AVG(CASE WHEN tt.category='CREDIT' THEN t.amount      END)        AS amt_avg_credit,
        MAX(CASE WHEN tt.category='DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_max_single_debit,
        MAX(CASE WHEN tt.category='CREDIT' THEN t.amount      ELSE 0 END) AS amt_max_single_credit,
        COUNT(DISTINCT t.merchant_name)                              AS distinct_merchants,
        MAX(tc.merchant_category)                                    AS top_merchant_category,
        CAST(SUM(CASE WHEN t.channel_code='ATM' THEN 1 ELSE 0 END)*100.0
             / GREATEST(COUNT(*),1) AS DECIMAL(5,2))                 AS pct_atm,
        CAST(SUM(CASE WHEN t.channel_code='POS' THEN 1 ELSE 0 END)*100.0
             / GREATEST(COUNT(*),1) AS DECIMAL(5,2))                 AS pct_pos,
        CAST(SUM(CASE WHEN t.channel_code='WEB' THEN 1 ELSE 0 END)*100.0
             / GREATEST(COUNT(*),1) AS DECIMAL(5,2))                 AS pct_web,
        CAST(SUM(CASE WHEN t.channel_code='MOB' THEN 1 ELSE 0 END)*100.0
             / GREATEST(COUNT(*),1) AS DECIMAL(5,2))                 AS pct_mobile,
        CAST(current_date - MAX(t.transaction_date) AS INTEGER)      AS days_since_last_txn,
        current_timestamp                                            AS load_ts
    FROM txn_processing.transactions t
    JOIN core_banking.accounts acct        ON t.account_id = acct.account_id
    JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
    CROSS JOIN run_params rp
    LEFT JOIN top_cat tc                   ON t.account_id = tc.account_id
    WHERE t.transaction_date BETWEEN rp.period_start AND rp.period_end
      AND t.status_code = 'P'
    GROUP BY acct.customer_id, acct.account_id, acct.account_type,
             rp.period_start, rp.period_end, tc.merchant_category
    """)
    cnt = con.execute("SELECT count(*) FROM etl_staging.stg_txn_summary").fetchone()[0]
    print(f"        → {cnt:,} rows")

    # -------------------------------------------------------------------
    # 2c  STG_RISK_FACTORS  (03_stg_risk_factors.bteq)
    # -------------------------------------------------------------------
    print("    [2c] Building etl_staging.stg_risk_factors …")

    # Work table 1: daily balance snapshots (last 3 months)
    con.execute("""
    CREATE OR REPLACE TEMP TABLE wrk_daily_balance AS
    SELECT acct.customer_id, t.account_id, t.transaction_date, t.running_balance AS eod_balance
    FROM txn_processing.transactions t
    JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    WHERE t.transaction_date >= current_date - INTERVAL '3' MONTH
      AND t.status_code = 'P'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.account_id, t.transaction_date
        ORDER BY t.transaction_ts DESC) = 1
    """)

    # Work table 2: payment behaviour on credit / loan accounts
    con.execute("""
    CREATE OR REPLACE TEMP TABLE wrk_payment_history AS
    SELECT
        acct.customer_id, acct.account_id,
        COUNT(*)                                                AS total_payments,
        SUM(CASE WHEN t.transaction_date <= acct.open_date
                      + INTERVAL (datediff('month', acct.open_date, t.transaction_date) + 1) MONTH
             THEN 1 ELSE 0 END)                                 AS ontime_payments,
        SUM(CASE WHEN t.transaction_date > acct.open_date
                      + INTERVAL (datediff('month', acct.open_date, t.transaction_date) + 1) MONTH
             THEN 1 ELSE 0 END)                                 AS late_payments,
        CAST(datediff('month',
             COALESCE(MAX(CASE
                 WHEN t.transaction_date > acct.open_date
                      + INTERVAL (datediff('month', acct.open_date, t.transaction_date) + 1) MONTH
                 THEN t.transaction_date END), MIN(acct.open_date)),
             current_date) AS INTEGER)                           AS months_since_last_late
    FROM txn_processing.transactions t
    JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
    WHERE acct.account_type IN ('CREDIT','LOAN')
      AND tt.category = 'CREDIT'
      AND t.status_code = 'P'
      AND t.transaction_date >= current_date - INTERVAL '24' MONTH
    GROUP BY acct.customer_id, acct.account_id
    """)

    # Final risk factors table
    con.execute("""
    CREATE OR REPLACE TABLE etl_staging.stg_risk_factors AS
    SELECT
        c.customer_id,
        COALESCE(overdraft.overdraft_count, 0)     AS account_overdraft_cnt,
        COALESCE(overdraft.nsf_total, 0.00)        AS nsf_fee_total,
        COALESCE(lg_wd.large_wd_cnt, 0)            AS large_withdrawal_cnt,
        COALESCE(lg_wd.large_wd_amt, 0.00)         AS large_withdrawal_amt,
        COALESCE(bal.avg_bal_30d, 0.00)             AS avg_daily_balance_30d,
        COALESCE(bal.avg_bal_90d, 0.00)             AS avg_daily_balance_90d,
        COALESCE(bal.bal_stddev, 0.0000)            AS balance_volatility,
        CASE WHEN credit.total_credit_limit > 0
             THEN CAST(credit.total_credit_bal / credit.total_credit_limit AS DECIMAL(5,4))
             ELSE 0.0000 END                        AS credit_util_ratio,
        CASE WHEN pmh.total_payments > 0
             THEN CAST(pmh.ontime_payments*100.0 / pmh.total_payments AS DECIMAL(5,2))
             ELSE 100.00 END                        AS payment_ontime_pct,
        COALESCE(pmh.late_payments, 0)              AS payment_late_cnt,
        COALESCE(pmh.months_since_last_late, 999)   AS months_since_last_late,
        COALESCE(bureau.credit_score, 0)            AS external_credit_score,
        COALESCE(vel.debit_7d, 0.00)               AS debit_velocity_7d,
        COALESCE(vel.debit_30d, 0.00)              AS debit_velocity_30d,
        COALESCE(merch.new_merch_30d, 0)           AS new_merchant_cnt_30d,
        COALESCE(merch.intl_txn_cnt, 0)            AS international_txn_cnt,
        COALESCE(merch.high_risk_cnt, 0)           AS high_risk_merchant_cnt,
        current_timestamp                           AS load_ts
    FROM core_banking.customers c

    LEFT JOIN (
        SELECT acct.customer_id,
            SUM(CASE WHEN t.running_balance < 0 THEN 1 ELSE 0 END)     AS overdraft_count,
            SUM(CASE WHEN tt.category='FEE' AND tt.description LIKE '%NSF%'
                     THEN ABS(t.amount) ELSE 0 END)                     AS nsf_total
        FROM txn_processing.transactions t
        JOIN core_banking.accounts acct ON t.account_id = acct.account_id
        JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE t.transaction_date >= current_date - INTERVAL '12' MONTH AND t.status_code='P'
        GROUP BY acct.customer_id
    ) overdraft ON c.customer_id = overdraft.customer_id

    LEFT JOIN (
        SELECT acct.customer_id, COUNT(*) AS large_wd_cnt, SUM(ABS(t.amount)) AS large_wd_amt
        FROM txn_processing.transactions t
        JOIN core_banking.accounts acct ON t.account_id = acct.account_id
        JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE tt.category='DEBIT' AND ABS(t.amount)>=5000
          AND t.transaction_date >= current_date - INTERVAL '12' MONTH AND t.status_code='P'
        GROUP BY acct.customer_id
    ) lg_wd ON c.customer_id = lg_wd.customer_id

    LEFT JOIN (
        SELECT customer_id,
            AVG(CASE WHEN transaction_date >= current_date - 30 THEN eod_balance END) AS avg_bal_30d,
            AVG(CASE WHEN transaction_date >= current_date - 90 THEN eod_balance END) AS avg_bal_90d,
            STDDEV_POP(eod_balance) AS bal_stddev
        FROM wrk_daily_balance GROUP BY customer_id
    ) bal ON c.customer_id = bal.customer_id

    LEFT JOIN (
        SELECT customer_id,
            SUM(COALESCE(current_balance,0)) AS total_credit_bal,
            SUM(COALESCE(credit_limit,0))    AS total_credit_limit
        FROM core_banking.accounts
        WHERE account_type='CREDIT' AND account_status='O'
        GROUP BY customer_id
    ) credit ON c.customer_id = credit.customer_id

    LEFT JOIN (
        SELECT customer_id,
            SUM(total_payments)         AS total_payments,
            SUM(ontime_payments)        AS ontime_payments,
            SUM(late_payments)          AS late_payments,
            MIN(months_since_last_late) AS months_since_last_late
        FROM wrk_payment_history GROUP BY customer_id
    ) pmh ON c.customer_id = pmh.customer_id

    LEFT JOIN (
        SELECT customer_id, external_credit_score AS credit_score
        FROM core_banking.customer_bureau_scores
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY report_date DESC) = 1
    ) bureau ON c.customer_id = bureau.customer_id

    LEFT JOIN (
        SELECT acct.customer_id,
            SUM(CASE WHEN t.transaction_date >= current_date - 7
                     THEN ABS(t.amount) ELSE 0 END) AS debit_7d,
            SUM(CASE WHEN t.transaction_date >= current_date - 30
                     THEN ABS(t.amount) ELSE 0 END) AS debit_30d
        FROM txn_processing.transactions t
        JOIN core_banking.accounts acct ON t.account_id = acct.account_id
        JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        WHERE tt.category='DEBIT' AND t.transaction_date >= current_date - 30 AND t.status_code='P'
        GROUP BY acct.customer_id
    ) vel ON c.customer_id = vel.customer_id

    LEFT JOIN (
        SELECT acct.customer_id,
            COUNT(DISTINCT CASE
                WHEN t.transaction_date >= current_date - 30 THEN t.merchant_name END) AS new_merch_30d,
            SUM(CASE WHEN t.channel_code = 'INTL' THEN 1 ELSE 0 END)                  AS intl_txn_cnt,
            SUM(CASE WHEN t.merchant_category IN (
                'GAMBLING','WIRE_TRANSFER_INTL','CRYPTO_EXCHANGE','PAWN_SHOP')
                     THEN 1 ELSE 0 END)                                                 AS high_risk_cnt
        FROM txn_processing.transactions t
        JOIN core_banking.accounts acct ON t.account_id = acct.account_id
        WHERE t.transaction_date >= current_date - INTERVAL '6' MONTH AND t.status_code='P'
        GROUP BY acct.customer_id
    ) merch ON c.customer_id = merch.customer_id

    WHERE c.customer_status IN ('A','I')
    """)
    cnt = con.execute("SELECT count(*) FROM etl_staging.stg_risk_factors").fetchone()[0]
    print(f"        → {cnt:,} rows")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — PYTHON ANALYTICS  (replaces SAS)
# ═══════════════════════════════════════════════════════════════════════════════

# ---------- 3a Customer Segmentation (01_sas_customer_segments.sas) ----------

def _phase3a_customer_segments(con: duckdb.DuckDBPyConnection) -> None:
    print("    [3a] Customer Segmentation (KMeans k=5) …")
    df = con.execute("""
        SELECT customer_id, age, tenure_months, customer_status, segment_code,
               state_code, num_accounts, num_active_accounts,
               has_checking, has_savings, has_credit, has_loan,
               total_balance, total_credit_limit, credit_utilization_pct
        FROM etl_staging.stg_customer_360
        WHERE customer_status = 'A'
    """).fetchdf()

    # Feature engineering (mirrors SAS STEP 2)
    df["product_breadth"] = df[["has_checking", "has_savings", "has_credit", "has_loan"]].apply(
        lambda r: sum(1 for v in r if v == "Y") / 4.0, axis=1)
    df["tenure_group"] = pd.cut(df["tenure_months"], bins=[-1, 12, 36, 84, 9999],
                                labels=["NEW (<1yr)", "DEVELOPING (1-3yr)",
                                        "ESTABLISHED (3-7yr)", "LOYAL (7yr+)"])
    df["age_group"] = pd.cut(df["age"], bins=[0, 25, 41, 57, 76, 120],
                             labels=["GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER", "SILENT"])
    df["balance_tier"] = pd.cut(df["total_balance"],
                                bins=[-float("inf"), 1000, 10000, 100000, float("inf")],
                                labels=["LOW", "MODERATE", "AFFLUENT", "HIGH_NET_WORTH"])
    df["log_balance"] = np.log(np.maximum(df["total_balance"].fillna(0).astype(float), 1))
    df["acct_ratio"] = (df["num_active_accounts"].fillna(0).astype(float)
                        / np.maximum(df["num_accounts"].fillna(1).astype(float), 1))

    # Standardise + KMeans (mirrors SAS PROC STDIZE + PROC FASTCLUS)
    feat_cols = ["log_balance", "tenure_months", "credit_utilization_pct",
                 "product_breadth", "acct_ratio", "age"]
    X = df[feat_cols].fillna(0).astype(float).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    km = KMeans(n_clusters=5, max_iter=50, tol=0.001, n_init=10, random_state=42)
    df["cluster"] = km.fit_predict(X_scaled)

    # Label clusters by average balance descending (mirrors SAS STEP 5)
    labels = ["PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL",
              "CREDIT_DEPENDENT", "VALUE_BASIC"]
    cluster_avg = df.groupby("cluster")["log_balance"].mean().sort_values(ascending=False)
    label_map = {c: labels[i] for i, c in enumerate(cluster_avg.index)}
    df["segment_name"] = df["cluster"].map(label_map)
    df["segment_id"] = df["cluster"]
    df["subsegment_id"] = 0

    # Scores & flags (mirrors SAS STEP 6)
    df["lifetime_value_score"] = (
        df["log_balance"] * df["tenure_months"] * df["product_breadth"] * 10
    ).round(2)
    df["engagement_score"] = (df["acct_ratio"] * 100).round(2)
    df["digital_adoption_score"] = 0.0
    df["product_breadth_index"] = (df["product_breadth"] * 100).round(2)
    df["channel_preference"] = ""
    df["cross_sell_flag"] = np.where(
        (df["product_breadth"] < 0.50) & (df["acct_ratio"] >= 0.75), "Y", "N")
    df["upsell_flag"] = np.where(
        (df["balance_tier"] == "MODERATE") & (df["tenure_group"] != "NEW (<1yr)"), "Y", "N")
    df["retention_risk_flag"] = np.where(
        (df["acct_ratio"] < 0.50) & (df["tenure_months"] >= 60), "Y", "N")
    df["model_version"] = "SEG_V3.2"
    df["effective_date"] = date.today()
    df["load_ts"] = datetime.now()

    out_cols = ["customer_id", "segment_name", "segment_id", "subsegment_id",
                "lifetime_value_score", "engagement_score", "digital_adoption_score",
                "product_breadth_index", "tenure_group", "age_group", "balance_tier",
                "channel_preference", "cross_sell_flag", "upsell_flag",
                "retention_risk_flag", "model_version", "effective_date", "load_ts"]
    con.register("_seg_df", df[out_cols])
    con.execute("CREATE OR REPLACE TABLE data_products.customer_segments AS SELECT * FROM _seg_df")
    con.unregister("_seg_df")
    print(f"        → {len(df):,} rows written to data_products.customer_segments")


# ---------- 3b Transaction Analytics (02_sas_txn_analytics.sas) ----------

def _phase3b_txn_analytics(con: duckdb.DuckDBPyConnection) -> None:
    print("    [3b] Transaction Analytics …")
    df = con.execute("SELECT * FROM etl_staging.stg_txn_summary").fetchdf()

    # Aggregate account-level → customer-level (mirrors SAS STEP 2)
    cust = df.groupby("customer_id").agg(
        total_accounts=("account_id", "nunique"),
        active_accounts=("days_since_last_txn", lambda s: (s <= 30).sum()),
        total_transactions=("txn_count_total", "sum"),
        total_debit_amt=("amt_total_debit", "sum"),
        total_credit_amt=("amt_total_credit", "sum"),
        total_fees=("amt_total_fees", "sum"),
        top_spend_category=("top_merchant_category", "first"),
    ).reset_index()
    cust["net_cash_flow"] = cust["total_credit_amt"] - cust["total_debit_amt"]
    cust["avg_transaction_size"] = np.where(
        cust["total_transactions"] > 0,
        (cust["total_debit_amt"] + cust["total_credit_amt"]) / cust["total_transactions"], 0)

    # Digital %
    txn_digital = df.copy()
    txn_digital["digital_txns"] = txn_digital["txn_count_total"] * (
        txn_digital["pct_web"].fillna(0) + txn_digital["pct_mobile"].fillna(0)) / 100.0
    dig = txn_digital.groupby("customer_id").agg(
        dig_txns=("digital_txns", "sum"), tot=("txn_count_total", "sum")).reset_index()
    dig["digital_txn_pct"] = np.where(dig["tot"] > 0, dig["dig_txns"] / dig["tot"] * 100, 0)
    cust = cust.merge(dig[["customer_id", "digital_txn_pct"]], on="customer_id", how="left")

    # Spend trend (mirrors SAS STEP 3)
    cust["monthly_spend_trend"] = np.where(
        cust["net_cash_flow"] > cust["avg_transaction_size"] * 5, "UP",
        np.where(cust["net_cash_flow"] < -cust["avg_transaction_size"] * 5, "DOWN", "STABLE"))
    cust["fee_income"] = cust["total_fees"]
    cust["interest_income"] = (cust["total_debit_amt"] * 0.02).round(2)
    cust["revenue_contribution"] = cust["fee_income"] + cust["interest_income"]

    # Percentile ranking (mirrors SAS PROC RANK)
    cust["spend_percentile"] = cust["total_debit_amt"].rank(pct=True).mul(100).round(2)

    # IQR anomaly detection (mirrors SAS PROC MEANS + data step)
    q1 = cust["total_debit_amt"].quantile(0.25)
    q3 = cust["total_debit_amt"].quantile(0.75)
    iqr = q3 - q1
    median = cust["total_debit_amt"].median()
    cust["anomaly_flag"] = np.where(
        (cust["total_debit_amt"] > median + 3 * iqr) & (iqr > 0), "Y", "N")

    # Metadata
    cust["reporting_period"] = date.today().strftime("%Y-%m")
    cust["model_version"] = "TXN_V2.1"
    cust["effective_date"] = date.today()
    cust["load_ts"] = datetime.now()

    con.register("_txn_df", cust)
    con.execute("CREATE OR REPLACE TABLE data_products.transaction_analytics AS SELECT * FROM _txn_df")
    con.unregister("_txn_df")
    print(f"        → {len(cust):,} rows written to data_products.transaction_analytics")


# ---------- 3c Risk Scoring (03_sas_risk_scoring.sas) ----------

def _phase3c_risk_scoring(con: duckdb.DuckDBPyConnection) -> None:
    print("    [3c] Risk Scoring (Logistic Regression) …")
    df = con.execute("""
        SELECT r.*, c.tenure_months, c.num_active_accounts, c.total_balance, c.customer_status
        FROM etl_staging.stg_risk_factors r
        JOIN etl_staging.stg_customer_360 c ON r.customer_id = c.customer_id
        WHERE c.customer_status = 'A'
    """).fetchdf()

    # Feature prep (mirrors SAS STEP 2)
    df["external_credit_score"] = df["external_credit_score"].replace(0, 680).fillna(680)
    df["bureau_score_norm"] = (df["external_credit_score"] - 300) / (850 - 300) * 100
    df["balance_trend_ratio"] = np.where(
        df["avg_daily_balance_90d"] > 0,
        df["avg_daily_balance_30d"] / df["avg_daily_balance_90d"], 1.0)
    df["velocity_ratio"] = np.where(
        df["debit_velocity_30d"] > 0,
        (df["debit_velocity_7d"] * (30 / 7)) / df["debit_velocity_30d"], 1.0)
    df["default_flag"] = (df["payment_late_cnt"] > 2).astype(int)

    # Logistic regression (mirrors SAS PROC LOGISTIC)
    feat_cols = ["bureau_score_norm", "credit_util_ratio", "payment_ontime_pct",
                 "balance_volatility", "velocity_ratio", "account_overdraft_cnt",
                 "large_withdrawal_cnt", "high_risk_merchant_cnt", "tenure_months"]
    X = df[feat_cols].fillna(0).astype(float).values
    y = df["default_flag"].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    try:
        lr = LogisticRegression(max_iter=200, solver="lbfgs", random_state=42)
        lr.fit(X_scaled, y)
        df["prob_default"] = lr.predict_proba(X_scaled)[:, 1]
    except Exception:
        # If only one class present, set default probability heuristically
        df["prob_default"] = df["default_flag"] * 0.8 + 0.05

    # Composite score & tier (mirrors SAS STEP 4)
    df["credit_risk_component"] = np.clip(100 - df["bureau_score_norm"], 0, 100)
    df["behaviour_risk_component"] = np.clip(100 - df["payment_ontime_pct"], 0, 100)
    df["velocity_risk_component"] = np.clip((df["velocity_ratio"] - 1) * 50, 0, 100)
    df["bureau_score_component"] = np.clip(df["bureau_score_norm"], 0, 100)
    df["payment_history_component"] = np.clip(df["payment_ontime_pct"], 0, 100)

    df["composite_risk_score"] = (
        df["credit_risk_component"]    * 0.30 +
        df["behaviour_risk_component"] * 0.25 +
        df["velocity_risk_component"]  * 0.15 +
        (100 - df["bureau_score_component"])  * 0.20 +
        (100 - df["payment_history_component"]) * 0.10
    ).round(2)

    df["probability_of_default"] = df["prob_default"].round(6)

    df["risk_tier"] = pd.cut(df["composite_risk_score"],
                             bins=[-0.01, 20, 40, 60, 80, 100.01],
                             labels=["LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"])

    # Primary & secondary risk drivers (mirrors SAS array logic)
    driver_cols = {
        "credit_risk_component": "CREDIT_UTILIZATION",
        "behaviour_risk_component": "PAYMENT_BEHAVIOUR",
        "velocity_risk_component": "TRANSACTION_VELOCITY",
    }
    inv_bureau = 100 - df["bureau_score_component"]
    driver_df = df[list(driver_cols.keys())].copy()
    driver_df["inv_bureau"] = inv_bureau
    driver_labels = list(driver_cols.values()) + ["BUREAU_SCORE"]

    def _top2(row):
        vals = row.values
        idx = np.argsort(vals)[::-1]
        return driver_labels[idx[0]], driver_labels[idx[1]]

    drivers = driver_df.apply(_top2, axis=1, result_type="expand")
    df["primary_risk_driver"] = drivers[0]
    df["secondary_risk_driver"] = drivers[1]

    df["score_delta_30d"] = 0.0
    df["watch_list_flag"] = np.where(
        (df["risk_tier"] == "CRITICAL") & (df["probability_of_default"] > 0.5), "Y", "N")
    df["review_required_flag"] = np.where(
        (df["composite_risk_score"] >= 60) & (df["velocity_ratio"] > 2.0), "Y", "N")
    df["model_version"] = "RISK_V4.0"
    df["effective_date"] = date.today()
    df["load_ts"] = datetime.now()

    out_cols = ["customer_id", "composite_risk_score", "risk_tier", "probability_of_default",
                "credit_risk_component", "behaviour_risk_component", "velocity_risk_component",
                "bureau_score_component", "payment_history_component",
                "primary_risk_driver", "secondary_risk_driver", "score_delta_30d",
                "watch_list_flag", "review_required_flag", "model_version",
                "effective_date", "load_ts"]
    con.register("_risk_df", df[out_cols])
    con.execute("CREATE OR REPLACE TABLE data_products.customer_risk_scores AS SELECT * FROM _risk_df")
    con.unregister("_risk_df")
    print(f"        → {len(df):,} rows written to data_products.customer_risk_scores")


# ---------- 3d Master Profile (04_sas_data_products.sas) ----------

def _phase3d_master_profile(con: duckdb.DuckDBPyConnection) -> None:
    print("    [3d] Master Profile (golden record) …")
    con.execute("""
    CREATE OR REPLACE TABLE data_products.customer_master_profile AS
    SELECT
        b.customer_id,
        b.first_name || ' ' || b.last_name                       AS full_name,
        b.age,
        b.state_code,
        b.customer_since,
        b.tenure_months,
        b.customer_status,
        COALESCE(s.segment_name, 'UNCLASSIFIED')                 AS segment_name,
        COALESCE(s.lifetime_value_score, 0)                      AS lifetime_value_score,
        COALESCE(s.engagement_score, 0)                          AS engagement_score,
        b.num_accounts                                           AS total_accounts,
        b.num_active_accounts                                    AS active_accounts,
        b.total_balance,
        b.total_credit_limit,
        b.credit_utilization_pct,
        COALESCE(t.total_transactions, 0)                        AS monthly_transactions,
        COALESCE(t.total_debit_amt, 0)                           AS monthly_spend,
        COALESCE(t.net_cash_flow, 0)                             AS net_cash_flow,
        t.top_spend_category,
        COALESCE(t.digital_txn_pct, 0)                           AS digital_txn_pct,
        r.composite_risk_score,
        COALESCE(CAST(r.risk_tier AS VARCHAR), 'UNKNOWN')        AS risk_tier,
        r.probability_of_default,
        COALESCE(r.watch_list_flag, 'N')                         AS watch_list_flag,
        COALESCE(s.cross_sell_flag, 'N')                         AS cross_sell_flag,
        COALESCE(s.upsell_flag, 'N')                             AS upsell_flag,
        COALESCE(s.retention_risk_flag, 'N')                     AS retention_risk_flag,
        'MASTER_V1.5'                                            AS model_version,
        current_date                                             AS effective_date,
        current_timestamp                                        AS load_ts
    FROM etl_staging.stg_customer_360 b
    LEFT JOIN data_products.customer_segments        s ON b.customer_id = s.customer_id
    LEFT JOIN data_products.transaction_analytics    t ON b.customer_id = t.customer_id
    LEFT JOIN data_products.customer_risk_scores     r ON b.customer_id = r.customer_id
    WHERE b.customer_status = 'A'
    """)
    cnt = con.execute("SELECT count(*) FROM data_products.customer_master_profile").fetchone()[0]
    print(f"        → {cnt:,} rows written to data_products.customer_master_profile")


def phase3_python_analytics(con: duckdb.DuckDBPyConnection) -> None:
    """Run all four SAS-replacement analytics in Python."""
    print("\n" + "=" * 72)
    print("PHASE 3: Python analytics (replacing SAS)")
    print("=" * 72)
    _phase3a_customer_segments(con)
    _phase3b_txn_analytics(con)
    _phase3c_risk_scoring(con)
    _phase3d_master_profile(con)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — SUMMARY REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def phase4_summary(con: duckdb.DuckDBPyConnection, elapsed: float) -> None:
    """Print a nicely formatted summary to stdout."""
    print("\n" + "=" * 72)
    print("PHASE 4: Summary Report")
    print("=" * 72)

    # Row counts
    tables = [
        ("etl_staging.stg_customer_360",        "STG_CUSTOMER_360"),
        ("etl_staging.stg_txn_summary",          "STG_TXN_SUMMARY"),
        ("etl_staging.stg_risk_factors",         "STG_RISK_FACTORS"),
        ("data_products.customer_segments",      "CUSTOMER_SEGMENTS"),
        ("data_products.transaction_analytics",  "TRANSACTION_ANALYTICS"),
        ("data_products.customer_risk_scores",   "CUSTOMER_RISK_SCORES"),
        ("data_products.customer_master_profile","CUSTOMER_MASTER_PROFILE"),
    ]
    print("\n  ┌─────────────────────────────────┬───────────┐")
    print("  │ Table                           │     Rows  │")
    print("  ├─────────────────────────────────┼───────────┤")
    for fqn, label in tables:
        cnt = con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
        print(f"  │ {label:<31} │ {cnt:>9,} │")
    print("  └─────────────────────────────────┴───────────┘")

    # Segment distribution
    seg = con.execute("""
        SELECT segment_name, COUNT(*) AS n,
               ROUND(AVG(lifetime_value_score),2) AS avg_ltv
        FROM data_products.customer_segments
        GROUP BY segment_name ORDER BY n DESC
    """).fetchdf()
    print("\n  Segment Distribution:")
    print("  " + "-" * 55)
    print(f"  {'Segment':<25} {'Count':>8}  {'Avg LTV':>10}")
    print("  " + "-" * 55)
    for _, r in seg.iterrows():
        print(f"  {r['segment_name']:<25} {r['n']:>8,}  {r['avg_ltv']:>10,.2f}")

    # Risk tier distribution
    risk = con.execute("""
        SELECT CAST(risk_tier AS VARCHAR) AS risk_tier, COUNT(*) AS n,
               ROUND(AVG(composite_risk_score),2) AS avg_score
        FROM data_products.customer_risk_scores
        GROUP BY risk_tier ORDER BY avg_score DESC
    """).fetchdf()
    print("\n  Risk Tier Distribution:")
    print("  " + "-" * 50)
    print(f"  {'Tier':<15} {'Count':>8}  {'Avg Score':>10}")
    print("  " + "-" * 50)
    for _, r in risk.iterrows():
        print(f"  {r['risk_tier']:<15} {r['n']:>8,}  {r['avg_score']:>10,.2f}")

    # Master profile sample
    sample = con.execute("""
        SELECT customer_id, full_name, segment_name,
               CAST(risk_tier AS VARCHAR) AS risk_tier,
               ROUND(composite_risk_score, 1) AS risk_score,
               ROUND(monthly_spend, 2) AS monthly_spend,
               watch_list_flag
        FROM data_products.customer_master_profile
        ORDER BY composite_risk_score DESC NULLS LAST
        LIMIT 10
    """).fetchdf()
    print("\n  Top-10 Customers by Risk Score:")
    print("  " + "-" * 95)
    print(f"  {'ID':>8}  {'Name':<25} {'Segment':<22} {'Tier':<12} {'Score':>6} {'Spend':>12} {'Watch':>5}")
    print("  " + "-" * 95)
    for _, r in sample.iterrows():
        print(f"  {r['customer_id']:>8}  {str(r['full_name'])[:24]:<25} "
              f"{str(r['segment_name'])[:21]:<22} {str(r['risk_tier']):<12} "
              f"{r['risk_score']:>6.1f} {r['monthly_spend']:>12,.2f} "
              f"{r['watch_list_flag']:>5}")

    print("\n  " + "=" * 50)
    print(f"  Total execution time: {elapsed:.2f}s")
    print("  " + "=" * 50)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║  DuckDB + Python Demo — Teradata BTEQ + SAS Pipeline Replacement   ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")

    t0 = time.time()
    con = duckdb.connect(":memory:")

    t1 = time.time()
    phase1_create_and_load(con)
    print(f"    ⏱  Phase 1 completed in {time.time()-t1:.2f}s")

    t2 = time.time()
    phase2_bteq_transforms(con)
    print(f"    ⏱  Phase 2 completed in {time.time()-t2:.2f}s")

    t3 = time.time()
    phase3_python_analytics(con)
    print(f"    ⏱  Phase 3 completed in {time.time()-t3:.2f}s")

    phase4_summary(con, time.time() - t0)
    con.close()


if __name__ == "__main__":
    main()
