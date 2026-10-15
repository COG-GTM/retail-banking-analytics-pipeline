# /// script
# requires-python = ">=3.10"
# dependencies = ["faker>=28.0", "duckdb>=1.0", "pandas>=2.0", "scikit-learn>=1.3", "numpy>=1.24"]
# ///
"""
Local DuckDB engine for the Retail Banking Analytics Pipeline.

Replaces the Teradata BTEQ + SAS legacy path with DuckDB SQL (staging) and
pandas / scikit-learn (data products). Used by:
  - orchestration/dags/retail_banking_pipeline_dag.py  (Airflow DAG, one task per step)
  - export_data.py                                     (CSV export of every layer)
  - `python local/duckdb/run_demo.py`                  (stand-alone end-to-end run)

Schemas mirror the Teradata databases:
  CORE_BANKING_DB   -> core_banking
  TXN_PROCESSING_DB -> txn_processing
  ETL_STAGING_DB    -> etl_staging
  DATA_PRODUCTS_DB  -> data_products
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

log = logging.getLogger("retail_banking.pipeline")

HERE = Path(__file__).resolve().parent
DEFAULT_DB_PATH = HERE / "airflow_local.db"
DB_PATH_ENV = "RETAIL_BANKING_DUCKDB_PATH"

SCHEMAS = ("core_banking", "txn_processing", "etl_staging", "data_products")
LOOKBACK_MONTHS = int(os.environ.get("LOOKBACK_MONTHS", "12"))
PAYMENT_GRACE_DAYS = 3
HIGH_RISK_CATEGORIES = ("GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP")

PRODUCT_TABLES = (
    "data_products.customer_segments",
    "data_products.transaction_analytics",
    "data_products.customer_risk_scores",
    "data_products.customer_master_profile",
)


class PipelineStepError(RuntimeError):
    """Raised when a step fails validation (BTEQ `.EXIT 99` / SAS `%abort cancel`)."""


# =============================================================================
# Connection / DDL
# =============================================================================
def resolve_db_path(path: str | os.PathLike | None = None) -> Path:
    return Path(path or os.environ.get(DB_PATH_ENV) or DEFAULT_DB_PATH).expanduser().resolve()


def connect(path: str | os.PathLike | None = None) -> duckdb.DuckDBPyConnection:
    db_path = resolve_db_path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


SOURCE_DDL = {
    "core_banking.customers": "customer_id BIGINT PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, date_of_birth DATE, ssn_hash VARCHAR, email VARCHAR, phone_primary VARCHAR, customer_since DATE, customer_status VARCHAR, segment_code VARCHAR, branch_id INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP",
    "core_banking.accounts": "account_id BIGINT PRIMARY KEY, customer_id BIGINT, account_type VARCHAR, account_status VARCHAR, open_date DATE, close_date DATE, current_balance DECIMAL(15,2), available_balance DECIMAL(15,2), credit_limit DECIMAL(15,2), interest_rate DECIMAL(5,4), branch_id INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP",
    "core_banking.addresses": "address_id BIGINT PRIMARY KEY, customer_id BIGINT, address_type VARCHAR, address_line_1 VARCHAR, address_line_2 VARCHAR, city VARCHAR, state_code VARCHAR, zip_code VARCHAR, country_code VARCHAR, is_primary VARCHAR, effective_date DATE, expiration_date DATE, created_ts TIMESTAMP, updated_ts TIMESTAMP",
    "txn_processing.transactions": "transaction_id BIGINT PRIMARY KEY, account_id BIGINT, transaction_type_cd VARCHAR, transaction_date DATE, transaction_ts TIMESTAMP, amount DECIMAL(15,2), running_balance DECIMAL(15,2), merchant_name VARCHAR, merchant_category VARCHAR, channel_code VARCHAR, reference_num VARCHAR, status_code VARCHAR, created_ts TIMESTAMP",
    "txn_processing.transaction_types": "transaction_type_cd VARCHAR PRIMARY KEY, description VARCHAR, category VARCHAR, is_revenue VARCHAR, effective_date DATE, expiration_date DATE",
    "core_banking.customer_bureau_scores": "customer_id BIGINT, external_credit_score INTEGER, report_date DATE",
}

STAGING_DDL = {
    "etl_staging.stg_customer_360": """
        customer_id BIGINT NOT NULL, first_name VARCHAR(60), last_name VARCHAR(60), date_of_birth DATE,
        age SMALLINT, customer_since DATE, tenure_months INTEGER, customer_status CHAR(1),
        segment_code VARCHAR(10), branch_id INTEGER, primary_address VARCHAR(200), city VARCHAR(60),
        state_code CHAR(2), zip_code VARCHAR(10), num_accounts SMALLINT, num_active_accounts SMALLINT,
        has_checking CHAR(1), has_savings CHAR(1), has_credit CHAR(1), has_loan CHAR(1),
        total_balance DECIMAL(18,2), total_credit_limit DECIMAL(18,2), credit_utilization_pct DECIMAL(5,2),
        load_ts TIMESTAMP""",
    "etl_staging.stg_txn_summary": """
        customer_id BIGINT NOT NULL, account_id BIGINT NOT NULL, account_type VARCHAR(20),
        summary_period_start DATE, summary_period_end DATE, txn_count_total INTEGER, txn_count_debit INTEGER,
        txn_count_credit INTEGER, txn_count_fee INTEGER, amt_total_debit DECIMAL(18,2),
        amt_total_credit DECIMAL(18,2), amt_total_fees DECIMAL(18,2), amt_avg_debit DECIMAL(15,2),
        amt_avg_credit DECIMAL(15,2), amt_max_single_debit DECIMAL(15,2), amt_max_single_credit DECIMAL(15,2),
        distinct_merchants INTEGER, top_merchant_category VARCHAR(60), pct_atm DECIMAL(5,2),
        pct_pos DECIMAL(5,2), pct_web DECIMAL(5,2), pct_mobile DECIMAL(5,2), days_since_last_txn INTEGER,
        load_ts TIMESTAMP""",
    # balance_volatility widened from DECIMAL(10,4): STDDEV_POP across a customer's
    # accounts can exceed 999,999 for high-net-worth customers.
    "etl_staging.stg_risk_factors": """
        customer_id BIGINT NOT NULL, account_overdraft_cnt INTEGER, nsf_fee_total DECIMAL(15,2),
        large_withdrawal_cnt INTEGER, large_withdrawal_amt DECIMAL(18,2), avg_daily_balance_30d DECIMAL(15,2),
        avg_daily_balance_90d DECIMAL(15,2), balance_volatility DECIMAL(18,4), credit_util_ratio DECIMAL(5,4),
        payment_ontime_pct DECIMAL(5,2), payment_late_cnt INTEGER, months_since_last_late INTEGER,
        external_credit_score INTEGER, debit_velocity_7d DECIMAL(15,2), debit_velocity_30d DECIMAL(15,2),
        new_merchant_cnt_30d INTEGER, international_txn_cnt INTEGER, high_risk_merchant_cnt INTEGER,
        load_ts TIMESTAMP""",
    "etl_staging.etl_run_log": """
        job_name VARCHAR, step_name VARCHAR, status VARCHAR, row_count BIGINT,
        start_ts TIMESTAMP, end_ts TIMESTAMP""",
}

PRODUCT_DDL = {
    "data_products.customer_segments": """
        customer_id BIGINT NOT NULL, segment_name VARCHAR(40), segment_id SMALLINT, subsegment_id SMALLINT,
        lifetime_value_score DECIMAL(10,2), engagement_score DECIMAL(5,2), digital_adoption_score DECIMAL(5,2),
        product_breadth_index DECIMAL(5,2), tenure_group VARCHAR(20), age_group VARCHAR(20),
        balance_tier VARCHAR(20), channel_preference VARCHAR(10), cross_sell_flag CHAR(1),
        upsell_flag CHAR(1), retention_risk_flag CHAR(1), model_version VARCHAR(20), effective_date DATE,
        load_ts TIMESTAMP""",
    "data_products.transaction_analytics": """
        customer_id BIGINT NOT NULL, reporting_period VARCHAR(7), total_accounts SMALLINT,
        active_accounts SMALLINT, total_transactions INTEGER, total_debit_amt DECIMAL(18,2),
        total_credit_amt DECIMAL(18,2), net_cash_flow DECIMAL(18,2), avg_transaction_size DECIMAL(15,2),
        monthly_spend_trend VARCHAR(10), spend_percentile DECIMAL(5,2), top_spend_category VARCHAR(60),
        digital_txn_pct DECIMAL(5,2), fee_income DECIMAL(15,2), interest_income DECIMAL(15,2),
        revenue_contribution DECIMAL(15,2), anomaly_flag CHAR(1), model_version VARCHAR(20),
        effective_date DATE, load_ts TIMESTAMP""",
    "data_products.customer_risk_scores": """
        customer_id BIGINT NOT NULL, composite_risk_score DECIMAL(6,2), risk_tier VARCHAR(20),
        probability_of_default DECIMAL(7,6), credit_risk_component DECIMAL(5,2),
        behaviour_risk_component DECIMAL(5,2), velocity_risk_component DECIMAL(5,2),
        bureau_score_component DECIMAL(5,2), payment_history_component DECIMAL(5,2),
        primary_risk_driver VARCHAR(40), secondary_risk_driver VARCHAR(40), score_delta_30d DECIMAL(6,2),
        watch_list_flag CHAR(1), review_required_flag CHAR(1), model_version VARCHAR(20),
        effective_date DATE, load_ts TIMESTAMP""",
    "data_products.customer_master_profile": """
        customer_id BIGINT NOT NULL, full_name VARCHAR(120), age SMALLINT, state_code CHAR(2),
        customer_since DATE, tenure_months INTEGER, customer_status CHAR(1), segment_name VARCHAR(40),
        lifetime_value_score DECIMAL(10,2), engagement_score DECIMAL(5,2), total_accounts SMALLINT,
        active_accounts SMALLINT, total_balance DECIMAL(18,2), total_credit_limit DECIMAL(18,2),
        credit_utilization_pct DECIMAL(5,2), monthly_transactions INTEGER, monthly_spend DECIMAL(18,2),
        net_cash_flow DECIMAL(18,2), top_spend_category VARCHAR(60), digital_txn_pct DECIMAL(5,2),
        composite_risk_score DECIMAL(6,2), risk_tier VARCHAR(20), probability_of_default DECIMAL(7,6),
        watch_list_flag CHAR(1), cross_sell_flag CHAR(1), upsell_flag CHAR(1), retention_risk_flag CHAR(1),
        model_version VARCHAR(20), effective_date DATE, load_ts TIMESTAMP""",
}


def create_schemas(con: duckdb.DuckDBPyConnection) -> None:
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"CREATE TABLE IF NOT EXISTS etl_staging.etl_run_log ({STAGING_DDL['etl_staging.etl_run_log']})")


def create_source_tables(con: duckdb.DuckDBPyConnection, replace: bool = False) -> None:
    create_schemas(con)
    verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    for table, cols in SOURCE_DDL.items():
        con.execute(f"{verb} {table} ({cols})")


def _recreate(con: duckdb.DuckDBPyConnection, table: str, ddl: dict[str, str]) -> None:
    con.execute(f"CREATE OR REPLACE TABLE {table} ({ddl[table]})")


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _write_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    """Insert a DataFrame into an existing table, casting to the declared column types."""
    cols = [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]
    con.register("_rb_frame", df[cols])
    try:
        con.execute(f"INSERT INTO {table} SELECT {', '.join(cols)} FROM _rb_frame")
    finally:
        con.unregister("_rb_frame")


def _run_logged(con: duckdb.DuckDBPyConnection, job_name: str, target: str, fn: Callable[[], None]) -> int:
    """Run a step, enforce the zero-row check, and append to etl_staging.etl_run_log."""
    create_schemas(con)
    start = datetime.now()
    log.info("[%s] START -> %s", job_name, target)
    fn()
    rows = _count(con, target)
    if rows == 0:
        # Equivalent of BTEQ `.IF ACTIVITYCOUNT = 0 THEN .EXIT 99`
        raise PipelineStepError(f"{job_name}: {target} has 0 rows")
    con.execute(
        "INSERT INTO etl_staging.etl_run_log VALUES (?, 'FULL_LOAD', 'SUCCESS', ?, ?, ?)",
        [job_name, rows, start, datetime.now()],
    )
    log.info("[%s] SUCCESS %s rows=%d (%.2fs)", job_name, target, rows, (datetime.now() - start).total_seconds())
    return rows


def _validate_frame(df: pd.DataFrame, name: str, key_cols: list[str], not_null: list[str], min_rows: int = 1) -> None:
    """Port of sas/macros/validate_table.sas (row count, key uniqueness, NOT NULL)."""
    if len(df) < min_rows:
        raise PipelineStepError(f"{name}: {len(df)} rows (minimum {min_rows})")
    dups = int(df.duplicated(subset=key_cols).sum())
    if dups:
        raise PipelineStepError(f"{name}: {dups} duplicate keys on {key_cols}")
    for col in not_null:
        nulls = int(df[col].isna().sum())
        if nulls:
            raise PipelineStepError(f"{name}: {nulls} NULL values in {col}")


# =============================================================================
# Phase 1: Synthetic source data
# =============================================================================
TRANSACTION_TYPES = [
    ("PUR", "Purchase", "DEBIT", "N"),
    ("WDR", "Withdrawal", "DEBIT", "N"),
    ("DEP", "Deposit", "CREDIT", "N"),
    ("PMT", "Payment", "CREDIT", "N"),
    ("FEE", "Account Fee", "FEE", "Y"),
    ("NSF", "NSF Fee", "FEE", "Y"),
    ("INT", "Interest", "CREDIT", "Y"),
    ("TRF", "Transfer", "DEBIT", "N"),
]

MERCHANT_CATEGORIES = ["GROCERY", "RETAIL", "RESTAURANT", "GAS_STATION", "UTILITIES",
                       "HEALTHCARE", "TRAVEL", "ENTERTAINMENT"]

# Per account type: (mean general transactions per year, {type_cd: probability})
TXN_PROFILE = {
    "CHECKING": (90, {"PUR": 0.52, "WDR": 0.15, "DEP": 0.16, "TRF": 0.10, "FEE": 0.05, "INT": 0.02}),
    "SAVINGS":  (24, {"DEP": 0.45, "WDR": 0.20, "TRF": 0.22, "INT": 0.10, "FEE": 0.03}),
    # Inflows on CREDIT/LOAN accounts come only from the scheduled PMT stream below.
    "CREDIT":   (60, {"PUR": 0.95, "FEE": 0.05}),
    "LOAN":     (2,  {"FEE": 1.0}),
}

CHANNELS = {
    "PUR": (["POS", "WEB", "MOB", "INTL"], [0.45, 0.25, 0.25, 0.05]),
    "WDR": (["ATM", "MOB", "WEB"], [0.80, 0.10, 0.10]),
    "DEP": (["ACH", "MOB", "ATM"], [0.50, 0.30, 0.20]),
    "TRF": (["WEB", "MOB", "WIRE"], [0.40, 0.40, 0.20]),
    "FEE": (["ACH"], [1.0]),
    "NSF": (["ACH"], [1.0]),
    "INT": (["ACH"], [1.0]),
    "PMT": (["ACH", "WEB", "MOB"], [0.40, 0.30, 0.30]),
}


def _random_dates(rng: np.random.Generator, start: np.ndarray, end: np.ndarray) -> np.ndarray:
    """Uniform random datetime64[D] between start and end (inclusive), element-wise."""
    span = (end - start).astype("timedelta64[D]").astype(np.int64)
    offs = (rng.random(len(span)) * (np.maximum(span, 0) + 1)).astype(np.int64)
    return start + offs.astype("timedelta64[D]")


def _builtin_populate_sources(con: duckdb.DuckDBPyConnection, n_customers: int = 5000, seed: int = 42) -> dict[str, int]:
    """Generate realistic synthetic rows for all six source tables (replaces existing rows)."""
    from faker import Faker

    if n_customers < 1:
        raise ValueError("n_customers must be >= 1")
    create_source_tables(con)
    for table in SOURCE_DDL:
        con.execute(f"DELETE FROM {table}")

    rng = np.random.default_rng(seed)
    fake = Faker("en_US")
    Faker.seed(seed)
    today = np.datetime64(date.today(), "D")
    now = pd.Timestamp(datetime.now())
    n = n_customers

    # ---- customers ----------------------------------------------------------
    cust_ids = np.arange(1, n + 1, dtype=np.int64)
    age_days = (rng.uniform(18, 86, n) * 365.25).astype(np.int64)
    dob = today - age_days.astype("timedelta64[D]")
    adult_since = dob + np.timedelta64(int(18 * 365.25), "D")
    earliest = np.maximum(adult_since, today - np.timedelta64(25 * 365, "D"))
    customer_since = _random_dates(rng, earliest, today - np.timedelta64(30, "D"))
    status = rng.choice(["A", "I", "C"], n, p=[0.82, 0.13, 0.05])
    # Latent risk propensity drives bureau score, late payments, NSF fees, risky merchants.
    risk = rng.beta(2, 8, n)
    first = [fake.first_name() for _ in range(n)]
    last = [fake.last_name() for _ in range(n)]
    customers = pd.DataFrame({
        "customer_id": cust_ids,
        "first_name": first,
        "last_name": last,
        "date_of_birth": dob,
        "ssn_hash": [hashlib.sha256(fake.ssn().encode()).hexdigest() for _ in range(n)],
        "email": [f"{f.lower()}.{l.lower()}{i}@example.com" for f, l, i in zip(first, last, cust_ids)],
        "phone_primary": [f"555-{a:03d}-{b:04d}" for a, b in zip(rng.integers(200, 999, n), rng.integers(0, 9999, n))],
        "customer_since": customer_since,
        "customer_status": status,
        "segment_code": rng.choice(["BASIC", "STANDARD", "PREFERRED", "PREMIER", "DIGITAL"], n),
        "branch_id": rng.integers(100, 500, n),
        "created_ts": now,
        "updated_ts": now,
    })

    # ---- accounts -----------------------------------------------------------
    n_accts = rng.choice([1, 2, 3, 4], n, p=[0.20, 0.35, 0.30, 0.15])
    acct_cust_idx = np.repeat(np.arange(n), n_accts)
    types = np.array(["CHECKING", "SAVINGS", "CREDIT", "LOAN"])
    acct_type = np.concatenate([rng.choice(types, k, replace=False, p=[0.38, 0.27, 0.22, 0.13]) for k in n_accts])
    m = len(acct_cust_idx)
    acct_ids = np.arange(1, m + 1, dtype=np.int64)
    open_date = _random_dates(rng, customer_since[acct_cust_idx], today - np.timedelta64(14, "D"))
    acct_status = rng.choice(["O", "C", "F"], m, p=[0.86, 0.08, 0.06])
    acct_status[status[acct_cust_idx] == "C"] = "C"
    close_date = np.where(acct_status == "C", _random_dates(rng, open_date, today), np.datetime64("NaT"))
    r_acct = risk[acct_cust_idx]

    balance = np.zeros(m)
    limit = np.full(m, np.nan)
    is_chk, is_sav, is_crd, is_loan = (acct_type == t for t in types)
    balance[is_chk] = rng.lognormal(7.8, 1.0, is_chk.sum())
    balance[is_sav] = rng.lognormal(9.2, 1.3, is_sav.sum())
    limit[is_crd] = rng.choice([1000, 2500, 5000, 7500, 10000, 15000, 25000], is_crd.sum())
    util = np.clip(rng.beta(2, 5, is_crd.sum()) + r_acct[is_crd] * 0.6, 0, 1.1)
    balance[is_crd] = limit[is_crd] * util
    balance[is_loan] = rng.uniform(5000, 250000, is_loan.sum())
    balance = np.minimum(balance, 400000).round(2)
    balance[acct_status == "C"] = 0.0
    available = np.where(is_crd, np.nan_to_num(limit) - balance, np.where(is_loan, 0.0, balance * 0.9)).round(2)
    rate = np.select([is_chk, is_sav, is_crd, is_loan],
                     [rng.uniform(0.0005, 0.01, m), rng.uniform(0.01, 0.045, m),
                      rng.uniform(0.15, 0.2999, m), rng.uniform(0.04, 0.12, m)]).round(4)
    accounts = pd.DataFrame({
        "account_id": acct_ids,
        "customer_id": cust_ids[acct_cust_idx],
        "account_type": acct_type,
        "account_status": acct_status,
        "open_date": open_date,
        "close_date": close_date,
        "current_balance": balance,
        "available_balance": available,
        "credit_limit": limit,
        "interest_rate": rate,
        "branch_id": rng.integers(100, 500, m),
        "created_ts": now,
        "updated_ts": now,
    })

    # ---- addresses (HOME + MAIL, plus expired / superseded HOME rows) --------
    addr_rows = []

    def _addr(cust_id: int, addr_type: str, eff, exp, primary: str) -> None:
        addr_rows.append((cust_id, addr_type, fake.street_address(),
                          fake.secondary_address() if rng.random() < 0.3 else None,
                          fake.city(), fake.state_abbr(include_territories=False), fake.zipcode(),
                          "US", primary, eff, exp))

    for i in range(n):
        cid = int(cust_ids[i])
        since = customer_since[i]
        home_eff = _random_dates(rng, np.array([since]), np.array([today]))[0]
        _addr(cid, "HOME", home_eff, None, "Y")
        _addr(cid, "MAIL", _random_dates(rng, np.array([since]), np.array([today]))[0], None, "N")
        roll = rng.random()
        if roll < 0.10:    # moved: previous HOME address expired
            _addr(cid, "HOME", since, home_eff, "N")
        elif roll < 0.15:  # older still-open HOME row; dedup must keep the latest
            _addr(cid, "HOME", since, None, "N")
    addresses = pd.DataFrame(addr_rows, columns=[
        "customer_id", "address_type", "address_line_1", "address_line_2", "city", "state_code",
        "zip_code", "country_code", "is_primary", "effective_date", "expiration_date"])
    addresses.insert(0, "address_id", np.arange(1, len(addresses) + 1, dtype=np.int64))
    addresses["created_ts"] = now
    addresses["updated_ts"] = now

    # ---- transaction types --------------------------------------------------
    txn_types = pd.DataFrame(TRANSACTION_TYPES, columns=["transaction_type_cd", "description", "category", "is_revenue"])
    txn_types["effective_date"] = pd.Timestamp("2020-01-01")
    txn_types["expiration_date"] = pd.NaT

    # ---- transactions: general activity over the last 12 months --------------
    year_ago = today - np.timedelta64(365, "D")
    lam = np.array([TXN_PROFILE[t][0] for t in acct_type], dtype=float)
    lam = lam * rng.uniform(0.4, 1.6, m)
    n_txn = rng.poisson(lam)
    t_acct = np.repeat(np.arange(m), n_txn)
    k = len(t_acct)
    t_type = np.empty(k, dtype=object)
    for at in types:
        mask = acct_type[t_acct] == at
        codes, probs = zip(*TXN_PROFILE[at][1].items())
        t_type[mask] = rng.choice(codes, mask.sum(), p=probs)
    t_risk = r_acct[t_acct]
    # Risky customers bounce more cheques
    nsf = (t_type == "FEE") & (rng.random(k) < 0.25 + t_risk)
    t_type[nsf] = "NSF"
    t_date = _random_dates(rng, np.maximum(open_date[t_acct], year_ago), np.full(k, today))

    amount = np.zeros(k)
    sel = t_type == "PUR"; amount[sel] = -rng.lognormal(3.7, 1.0, sel.sum())
    sel = t_type == "WDR"; amount[sel] = -rng.choice([20, 40, 60, 100, 200, 300, 500, 800], sel.sum())
    big = sel & (rng.random(k) < 0.01 + t_risk * 0.08)
    amount[big] = -rng.uniform(5000, 15000, big.sum())
    sel = t_type == "DEP"; amount[sel] = rng.lognormal(5.9, 0.8, sel.sum())
    sel = t_type == "TRF"; amount[sel] = -rng.lognormal(5.8, 0.9, sel.sum())
    sel = t_type == "FEE"; amount[sel] = -rng.choice([5, 10, 12, 15, 25, 35], sel.sum())
    sel = t_type == "NSF"; amount[sel] = -35.0
    sel = t_type == "INT"; amount[sel] = rng.uniform(0.5, 60, sel.sum())

    # Merchants: a fixed pool with categories; risky customers favour high-risk merchants.
    n_merchants = 400
    merch_names = np.array([fake.unique.company() for _ in range(n_merchants)], dtype=object)
    merch_cats = np.array(rng.choice(MERCHANT_CATEGORIES, n_merchants), dtype=object)
    hr_names = np.array([fake.unique.company() for _ in range(40)], dtype=object)
    hr_cats = np.array(rng.choice(HIGH_RISK_CATEGORIES, 40), dtype=object)
    merchant = np.full(k, None, dtype=object)
    mcat = np.full(k, None, dtype=object)
    pur = t_type == "PUR"
    # Each account shops at a small personal set of merchants (so "new merchant" signals are meaningful)
    pref = rng.integers(0, n_merchants, size=(m, 12))
    pick = pref[t_acct, rng.integers(0, 12, k)]
    roam = rng.random(k) < 0.15
    pick = np.where(roam, rng.integers(0, n_merchants, k), pick)
    merchant[pur] = merch_names[pick[pur]]
    mcat[pur] = merch_cats[pick[pur]]
    hr = pur & (rng.random(k) < t_risk * 0.25)
    hr_pick = rng.integers(0, 40, k)
    merchant[hr] = hr_names[hr_pick[hr]]
    mcat[hr] = hr_cats[hr_pick[hr]]

    general = pd.DataFrame({"acct_idx": t_acct, "transaction_type_cd": t_type, "transaction_date": t_date,
                            "amount": amount, "merchant_name": merchant, "merchant_category": mcat})

    # ---- transactions: scheduled monthly payments on CREDIT / LOAN accounts ---
    pay_rows = []
    two_years_ago = date.today() - timedelta(days=730)
    for idx in np.flatnonzero(is_crd | is_loan):
        od = pd.Timestamp(open_date[idx]).date()
        due_day = min(od.day, 28)
        p_late = min(0.6, 0.005 + float(r_acct[idx]) ** 2 * 0.8)
        installment = balance[idx] * (0.03 if is_crd[idx] else 0.015) + 25
        y, mo = od.year, od.month
        while True:
            mo += 1
            if mo > 12:
                y, mo = y + 1, 1
            due = date(y, mo, due_day)
            if due > date.today():
                break
            if due < two_years_ago:
                continue
            delay = int(rng.integers(5, 21)) if rng.random() < p_late else int(rng.integers(0, PAYMENT_GRACE_DAYS + 1))
            paid = due + timedelta(days=delay)
            if paid <= date.today():
                pay_rows.append((idx, "PMT", np.datetime64(paid, "D"), round(installment * rng.uniform(0.8, 1.2), 2)))
    payments = pd.DataFrame(pay_rows, columns=["acct_idx", "transaction_type_cd", "transaction_date", "amount"])
    payments["merchant_name"] = None
    payments["merchant_category"] = None
    general["transaction_date"] = pd.to_datetime(general["transaction_date"]).astype("datetime64[ns]")
    payments["transaction_date"] = pd.to_datetime(payments["transaction_date"]).astype("datetime64[ns]")

    txns = pd.concat([general, payments], ignore_index=True)
    k = len(txns)
    codes = txns["transaction_type_cd"].to_numpy()
    channel = np.empty(k, dtype=object)
    for cd, (chs, probs) in CHANNELS.items():
        mask = codes == cd
        channel[mask] = rng.choice(chs, mask.sum(), p=probs)
    txns["channel_code"] = channel
    txns["transaction_ts"] = pd.to_datetime(txns["transaction_date"]) + pd.to_timedelta(rng.integers(6 * 3600, 23 * 3600, k), unit="s")
    txns["status_code"] = rng.choice(["P", "H", "R"], k, p=[0.92, 0.04, 0.04])
    txns["status_code"] = np.where(txns["transaction_type_cd"].isin(["PMT", "FEE", "NSF", "INT"]), "P", txns["status_code"])
    txns["amount"] = txns["amount"].round(2)
    txns = txns.sort_values(["transaction_ts", "acct_idx"], kind="stable").reset_index(drop=True)
    txns["transaction_id"] = np.arange(1, k + 1, dtype=np.int64)
    txns["account_id"] = acct_ids[txns["acct_idx"].to_numpy()]
    # Running balance ends at the account's current balance.
    posted_amt = txns["amount"].where(txns["status_code"] == "P", 0.0)
    cum = posted_amt.groupby(txns["account_id"]).cumsum()
    total = posted_amt.groupby(txns["account_id"]).transform("sum")
    txns["running_balance"] = (balance[txns["acct_idx"].to_numpy()] - total + cum).round(2)
    txns["reference_num"] = [f"REF{i:010d}" for i in txns["transaction_id"]]
    txns["created_ts"] = now

    # ---- bureau scores: 1-3 reports per customer, ~3% have none ---------------
    n_reports = rng.choice([0, 1, 2, 3], n, p=[0.03, 0.47, 0.30, 0.20])
    b_idx = np.repeat(np.arange(n), n_reports)
    base_score = 790 - risk * 420
    bureau = pd.DataFrame({
        "customer_id": cust_ids[b_idx],
        "external_credit_score": np.clip(rng.normal(base_score[b_idx], 25), 300, 850).round().astype(int),
        "report_date": today - rng.integers(1, 365, len(b_idx)).astype("timedelta64[D]"),
    })

    for table, df in [
        ("core_banking.customers", customers),
        ("core_banking.accounts", accounts),
        ("core_banking.addresses", addresses),
        ("txn_processing.transaction_types", txn_types),
        ("txn_processing.transactions", txns),
        ("core_banking.customer_bureau_scores", bureau),
    ]:
        _write_df(con, table, df)

    counts = {t: _count(con, t) for t in SOURCE_DDL}
    for t, c in counts.items():
        log.info("[generate_source_data] %-40s %10d rows", t, c)
    return counts


def generate_source_data(con: duckdb.DuckDBPyConnection, n_customers: int = 5000, seed: int = 42) -> dict[str, int]:
    """Phase 1 for the DAG: (re)create source tables and populate them."""
    create_source_tables(con, replace=True)
    counts = _builtin_populate_sources(con, n_customers=n_customers, seed=seed)
    empty = [t for t, c in counts.items() if c == 0]
    if empty:
        raise PipelineStepError(f"generate_source_data: empty source tables {empty}")
    return counts


# =============================================================================
# Phase 2: BTEQ staging transforms (DuckDB SQL ports of bteq/*.bteq)
# =============================================================================
SQL_STG_CUSTOMER_360 = """
INSERT INTO etl_staging.stg_customer_360
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.date_of_birth,
    CAST(FLOOR((CURRENT_DATE - c.date_of_birth) / 365.25) AS SMALLINT)       AS age,
    c.customer_since,
    CAST(date_sub('month', c.customer_since, CURRENT_DATE) AS INTEGER)        AS tenure_months,
    c.customer_status,
    c.segment_code,
    c.branch_id,
    TRIM(a.address_line_1) || COALESCE(', ' || TRIM(a.address_line_2), '')    AS primary_address,
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
         THEN CAST(acct_agg.credit_balance / acct_agg.total_credit_limit * 100 AS DECIMAL(5,2))
         ELSE 0.00 END                                                        AS credit_utilization_pct,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                      AS load_ts
FROM core_banking.customers c
LEFT JOIN (
    SELECT customer_id, address_line_1, address_line_2, city, state_code, zip_code
    FROM core_banking.addresses
    WHERE address_type = 'HOME'
      AND (expiration_date IS NULL OR expiration_date > CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_date DESC) = 1
) a ON c.customer_id = a.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)                                                         AS num_accounts,
        SUM(CASE WHEN account_status = 'O' THEN 1 ELSE 0 END)            AS num_active_accounts,
        MAX(CASE WHEN account_type = 'CHECKING' THEN 'Y' ELSE 'N' END)   AS has_checking,
        MAX(CASE WHEN account_type = 'SAVINGS'  THEN 'Y' ELSE 'N' END)   AS has_savings,
        MAX(CASE WHEN account_type = 'CREDIT'   THEN 'Y' ELSE 'N' END)   AS has_credit,
        MAX(CASE WHEN account_type = 'LOAN'     THEN 'Y' ELSE 'N' END)   AS has_loan,
        SUM(COALESCE(current_balance, 0))                                AS total_balance,
        SUM(CASE WHEN account_type = 'CREDIT' THEN COALESCE(credit_limit, 0) ELSE 0 END)    AS total_credit_limit,
        SUM(CASE WHEN account_type = 'CREDIT' THEN COALESCE(current_balance, 0) ELSE 0 END) AS credit_balance
    FROM core_banking.accounts
    GROUP BY customer_id
) acct_agg ON c.customer_id = acct_agg.customer_id
WHERE c.customer_status IN ('A', 'I')
"""

SQL_STG_TXN_SUMMARY = """
INSERT INTO etl_staging.stg_txn_summary
WITH rp AS (
    SELECT CAST(CURRENT_DATE - INTERVAL {lookback} MONTH AS DATE) AS period_start,
           CURRENT_DATE                                         AS period_end
),
top_cat AS (
    SELECT account_id, merchant_category
    FROM (
        SELECT t2.account_id, t2.merchant_category, SUM(ABS(t2.amount)) AS cat_spend
        FROM txn_processing.transactions t2
        INNER JOIN rp rp2 ON t2.transaction_date BETWEEN rp2.period_start AND rp2.period_end
        WHERE t2.status_code = 'P' AND t2.merchant_category IS NOT NULL
        GROUP BY t2.account_id, t2.merchant_category
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY cat_spend DESC, merchant_category) = 1
)
SELECT
    acct.customer_id,
    acct.account_id,
    acct.account_type,
    rp.period_start                                                    AS summary_period_start,
    rp.period_end                                                      AS summary_period_end,
    COUNT(*)                                                           AS txn_count_total,
    SUM(CASE WHEN tt.category = 'DEBIT'  THEN 1 ELSE 0 END)            AS txn_count_debit,
    SUM(CASE WHEN tt.category = 'CREDIT' THEN 1 ELSE 0 END)            AS txn_count_credit,
    SUM(CASE WHEN tt.category = 'FEE'    THEN 1 ELSE 0 END)            AS txn_count_fee,
    SUM(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_total_debit,
    SUM(CASE WHEN tt.category = 'CREDIT' THEN t.amount ELSE 0 END)     AS amt_total_credit,
    SUM(CASE WHEN tt.category = 'FEE'    THEN ABS(t.amount) ELSE 0 END) AS amt_total_fees,
    AVG(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) END)       AS amt_avg_debit,
    AVG(CASE WHEN tt.category = 'CREDIT' THEN t.amount END)            AS amt_avg_credit,
    MAX(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_max_single_debit,
    MAX(CASE WHEN tt.category = 'CREDIT' THEN t.amount ELSE 0 END)     AS amt_max_single_credit,
    COUNT(DISTINCT t.merchant_name)                                    AS distinct_merchants,
    MAX(top_cat.merchant_category)                                     AS top_merchant_category,
    CAST(SUM(CASE WHEN t.channel_code = 'ATM' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_atm,
    CAST(SUM(CASE WHEN t.channel_code = 'POS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_pos,
    CAST(SUM(CASE WHEN t.channel_code = 'WEB' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_web,
    CAST(SUM(CASE WHEN t.channel_code IN ('MOB') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_mobile,
    CAST(CURRENT_DATE - MAX(t.transaction_date) AS INTEGER)            AS days_since_last_txn,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                               AS load_ts
FROM txn_processing.transactions t
INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
INNER JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
CROSS JOIN rp
LEFT JOIN top_cat ON t.account_id = top_cat.account_id
WHERE t.transaction_date BETWEEN rp.period_start AND rp.period_end
  AND t.status_code = 'P'
GROUP BY acct.customer_id, acct.account_id, acct.account_type, rp.period_start, rp.period_end,
         top_cat.merchant_category
"""

SQL_WRK_DAILY_BALANCE = """
CREATE OR REPLACE TABLE etl_staging.wrk_daily_balance AS
SELECT acct.customer_id, t.account_id, t.transaction_date, t.running_balance AS eod_balance
FROM txn_processing.transactions t
INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
WHERE t.transaction_date >= CAST(CURRENT_DATE - INTERVAL 3 MONTH AS DATE)
  AND t.status_code = 'P'
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.account_id, t.transaction_date ORDER BY t.transaction_ts DESC) = 1
"""

# Teradata's on-time expression in 03_stg_risk_factors.bteq
# (txn_date <= ADD_MONTHS(open_date, MONTHS_BETWEEN(txn_date, open_date)(INTEGER) + 1))
# is always true, so it can never flag a late payment. This port implements the
# documented intent: a payment is on time when posted within PAYMENT_GRACE_DAYS
# of the most recent monthly due date (the account-open day of month).
SQL_WRK_PAYMENT_HISTORY = """
CREATE OR REPLACE TABLE etl_staging.wrk_payment_history AS
WITH pmt AS (
    SELECT
        acct.customer_id,
        acct.account_id,
        acct.open_date,
        t.transaction_date,
        make_date(year(t.transaction_date), month(t.transaction_date), LEAST(day(acct.open_date), 28)) AS due_this_month
    FROM txn_processing.transactions t
    INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    INNER JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
    WHERE acct.account_type IN ('CREDIT', 'LOAN')
      AND tt.category = 'CREDIT'
      AND t.status_code = 'P'
      AND t.transaction_date >= CAST(CURRENT_DATE - INTERVAL 24 MONTH AS DATE)
),
dpd AS (
    SELECT *,
        transaction_date - CASE WHEN transaction_date >= due_this_month THEN due_this_month
                                ELSE CAST(due_this_month - INTERVAL 1 MONTH AS DATE) END AS days_past_due
    FROM pmt
)
SELECT
    customer_id,
    account_id,
    COUNT(*)                                                            AS total_payments,
    SUM(CASE WHEN days_past_due <= {grace} THEN 1 ELSE 0 END)           AS ontime_payments,
    SUM(CASE WHEN days_past_due >  {grace} THEN 1 ELSE 0 END)           AS late_payments,
    CAST(date_sub('month',
        COALESCE(MAX(CASE WHEN days_past_due > {grace} THEN transaction_date END), MIN(open_date)),
        CURRENT_DATE) AS INTEGER)                                       AS months_since_last_late
FROM dpd
GROUP BY customer_id, account_id
"""

SQL_STG_RISK_FACTORS = """
INSERT INTO etl_staging.stg_risk_factors
SELECT
    c.customer_id,
    COALESCE(overdraft.overdraft_count, 0)                      AS account_overdraft_cnt,
    COALESCE(overdraft.nsf_total, 0.00)                         AS nsf_fee_total,
    COALESCE(lg_wd.large_wd_cnt, 0)                             AS large_withdrawal_cnt,
    COALESCE(lg_wd.large_wd_amt, 0.00)                          AS large_withdrawal_amt,
    COALESCE(bal.avg_bal_30d, 0.00)                             AS avg_daily_balance_30d,
    COALESCE(bal.avg_bal_90d, 0.00)                             AS avg_daily_balance_90d,
    COALESCE(bal.bal_stddev, 0.0000)                            AS balance_volatility,
    CASE WHEN credit.total_credit_limit > 0
         THEN CAST(LEAST(credit.total_credit_bal / credit.total_credit_limit, 9.9999) AS DECIMAL(5,4))
         ELSE 0.0000 END                                        AS credit_util_ratio,
    CASE WHEN pmh.total_payments > 0
         THEN CAST(pmh.ontime_payments * 100.0 / pmh.total_payments AS DECIMAL(5,2))
         ELSE 100.00 END                                        AS payment_ontime_pct,
    COALESCE(pmh.late_payments, 0)                              AS payment_late_cnt,
    COALESCE(pmh.months_since_last_late, 999)                   AS months_since_last_late,
    COALESCE(bureau.credit_score, 0)                            AS external_credit_score,
    COALESCE(vel.debit_7d, 0.00)                                AS debit_velocity_7d,
    COALESCE(vel.debit_30d, 0.00)                               AS debit_velocity_30d,
    COALESCE(merch.new_merch_30d, 0)                            AS new_merchant_cnt_30d,
    COALESCE(merch.intl_txn_cnt, 0)                             AS international_txn_cnt,
    COALESCE(merch.high_risk_cnt, 0)                            AS high_risk_merchant_cnt,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                        AS load_ts
FROM core_banking.customers c
LEFT JOIN (
    SELECT acct.customer_id,
           SUM(CASE WHEN t.running_balance < 0 THEN 1 ELSE 0 END) AS overdraft_count,
           SUM(CASE WHEN tt.category = 'FEE' AND tt.description LIKE '%NSF%' THEN ABS(t.amount) ELSE 0 END) AS nsf_total
    FROM txn_processing.transactions t
    INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    INNER JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
    WHERE t.transaction_date >= CAST(CURRENT_DATE - INTERVAL 12 MONTH AS DATE)
      AND t.status_code = 'P'
    GROUP BY acct.customer_id
) overdraft ON c.customer_id = overdraft.customer_id
LEFT JOIN (
    SELECT acct.customer_id, COUNT(*) AS large_wd_cnt, SUM(ABS(t.amount)) AS large_wd_amt
    FROM txn_processing.transactions t
    INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    INNER JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
    WHERE tt.category = 'DEBIT'
      AND ABS(t.amount) >= 5000
      AND t.transaction_date >= CAST(CURRENT_DATE - INTERVAL 12 MONTH AS DATE)
      AND t.status_code = 'P'
    GROUP BY acct.customer_id
) lg_wd ON c.customer_id = lg_wd.customer_id
LEFT JOIN (
    SELECT customer_id,
           AVG(CASE WHEN transaction_date >= CURRENT_DATE - 30 THEN eod_balance END) AS avg_bal_30d,
           AVG(CASE WHEN transaction_date >= CURRENT_DATE - 90 THEN eod_balance END) AS avg_bal_90d,
           STDDEV_POP(eod_balance)                                                  AS bal_stddev
    FROM etl_staging.wrk_daily_balance
    GROUP BY customer_id
) bal ON c.customer_id = bal.customer_id
LEFT JOIN (
    SELECT customer_id,
           SUM(COALESCE(current_balance, 0)) AS total_credit_bal,
           SUM(COALESCE(credit_limit, 0))    AS total_credit_limit
    FROM core_banking.accounts
    WHERE account_type = 'CREDIT' AND account_status = 'O'
    GROUP BY customer_id
) credit ON c.customer_id = credit.customer_id
LEFT JOIN (
    SELECT customer_id,
           SUM(total_payments)         AS total_payments,
           SUM(ontime_payments)        AS ontime_payments,
           SUM(late_payments)          AS late_payments,
           MIN(months_since_last_late) AS months_since_last_late
    FROM etl_staging.wrk_payment_history
    GROUP BY customer_id
) pmh ON c.customer_id = pmh.customer_id
LEFT JOIN (
    SELECT customer_id, external_credit_score AS credit_score
    FROM core_banking.customer_bureau_scores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY report_date DESC) = 1
) bureau ON c.customer_id = bureau.customer_id
LEFT JOIN (
    SELECT acct.customer_id,
           SUM(CASE WHEN t.transaction_date >= CURRENT_DATE - 7  THEN ABS(t.amount) ELSE 0 END) AS debit_7d,
           SUM(CASE WHEN t.transaction_date >= CURRENT_DATE - 30 THEN ABS(t.amount) ELSE 0 END) AS debit_30d
    FROM txn_processing.transactions t
    INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    INNER JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
    WHERE tt.category = 'DEBIT'
      AND t.transaction_date >= CURRENT_DATE - 30
      AND t.status_code = 'P'
    GROUP BY acct.customer_id
) vel ON c.customer_id = vel.customer_id
LEFT JOIN (
    -- NOT IN (correlated) rewritten as an anti-join on merchants seen before the 30-day window
    SELECT acct.customer_id,
           COUNT(DISTINCT CASE WHEN t.transaction_date >= CURRENT_DATE - 30 AND prior.merchant_name IS NULL
                               THEN t.merchant_name END)                               AS new_merch_30d,
           SUM(CASE WHEN t.channel_code = 'INTL' THEN 1 ELSE 0 END)                     AS intl_txn_cnt,
           SUM(CASE WHEN t.merchant_category IN ({high_risk}) THEN 1 ELSE 0 END)       AS high_risk_cnt
    FROM txn_processing.transactions t
    INNER JOIN core_banking.accounts acct ON t.account_id = acct.account_id
    LEFT JOIN (
        SELECT DISTINCT account_id, merchant_name
        FROM txn_processing.transactions
        WHERE transaction_date < CURRENT_DATE - 30 AND merchant_name IS NOT NULL
    ) prior ON prior.account_id = t.account_id AND prior.merchant_name = t.merchant_name
    WHERE t.transaction_date >= CAST(CURRENT_DATE - INTERVAL 6 MONTH AS DATE)
      AND t.status_code = 'P'
    GROUP BY acct.customer_id
) merch ON c.customer_id = merch.customer_id
WHERE c.customer_status IN ('A', 'I')
"""


def stg_customer_360(con: duckdb.DuckDBPyConnection) -> int:
    """Port of bteq/01_stg_customer_360.bteq."""
    def run() -> None:
        _recreate(con, "etl_staging.stg_customer_360", STAGING_DDL)
        con.execute(SQL_STG_CUSTOMER_360)
    return _run_logged(con, "01_stg_customer_360", "etl_staging.stg_customer_360", run)


def stg_txn_summary(con: duckdb.DuckDBPyConnection, lookback_months: int = LOOKBACK_MONTHS) -> int:
    """Port of bteq/02_stg_txn_summary.bteq."""
    def run() -> None:
        _recreate(con, "etl_staging.stg_txn_summary", STAGING_DDL)
        con.execute(SQL_STG_TXN_SUMMARY.format(lookback=int(lookback_months)))
    return _run_logged(con, "02_stg_txn_summary", "etl_staging.stg_txn_summary", run)


def stg_risk_factors(con: duckdb.DuckDBPyConnection) -> int:
    """Port of bteq/03_stg_risk_factors.bteq (work tables are dropped afterwards)."""
    def run() -> None:
        con.execute(SQL_WRK_DAILY_BALANCE)
        con.execute(SQL_WRK_PAYMENT_HISTORY.format(grace=PAYMENT_GRACE_DAYS))
        _recreate(con, "etl_staging.stg_risk_factors", STAGING_DDL)
        con.execute(SQL_STG_RISK_FACTORS.format(high_risk=", ".join(f"'{c}'" for c in HIGH_RISK_CATEGORIES)))
        con.execute("DROP TABLE IF EXISTS etl_staging.wrk_daily_balance")
        con.execute("DROP TABLE IF EXISTS etl_staging.wrk_payment_history")
    return _run_logged(con, "03_stg_risk_factors", "etl_staging.stg_risk_factors", run)


def phase2_bteq_transforms(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        "etl_staging.stg_customer_360": stg_customer_360(con),
        "etl_staging.stg_txn_summary": stg_txn_summary(con),
        "etl_staging.stg_risk_factors": stg_risk_factors(con),
    }


# =============================================================================
# Phase 3: SAS analytics (pandas / scikit-learn ports of sas/*.sas)
# =============================================================================
def _yn(mask: pd.Series | np.ndarray) -> np.ndarray:
    return np.where(mask, "Y", "N")


def _min_rows() -> int:
    # SAS programs require >= 1000 rows; default to 1 so small local runs succeed.
    return int(os.environ.get("RETAIL_BANKING_MIN_PRODUCT_ROWS", "1"))


def customer_segments(con: duckdb.DuckDBPyConnection, seed: int = 42) -> int:
    """Port of sas/01_sas_customer_segments.sas (PROC STDIZE + PROC FASTCLUS -> KMeans)."""
    from sklearn.cluster import KMeans

    def run() -> None:
        df = con.execute("""
            SELECT customer_id, age, tenure_months, customer_status, segment_code, state_code,
                   num_accounts, num_active_accounts, has_checking, has_savings, has_credit, has_loan,
                   total_balance, total_credit_limit, credit_utilization_pct
            FROM etl_staging.stg_customer_360 WHERE customer_status = 'A'
        """).df()
        if df.empty:
            raise PipelineStepError("01_customer_segments: no active customers in stg_customer_360")
        for col in ("num_accounts", "num_active_accounts", "total_balance", "total_credit_limit", "credit_utilization_pct"):
            df[col] = df[col].fillna(0).astype(float)
        for col in ("has_checking", "has_savings", "has_credit", "has_loan"):
            df[col] = df[col].fillna("N")

        df["product_breadth"] = df[["has_checking", "has_savings", "has_credit", "has_loan"]].eq("Y").mean(axis=1)
        df["tenure_group"] = np.select(
            [df.tenure_months < 12, df.tenure_months < 36, df.tenure_months < 84],
            ["NEW (<1yr)", "DEVELOPING (1-3yr)", "ESTABLISHED (3-7yr)"], "LOYAL (7yr+)")
        df["age_group"] = np.select(
            [df.age < 25, df.age < 41, df.age < 57, df.age < 76],
            ["GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER"], "SILENT")
        df["balance_tier"] = np.select(
            [df.total_balance < 1000, df.total_balance < 10000, df.total_balance < 100000],
            ["LOW", "MODERATE", "AFFLUENT"], "HIGH_NET_WORTH")
        df["digital_adoption_score"] = 0.0
        df["log_balance"] = np.log(np.maximum(df.total_balance, 1))
        df["acct_ratio"] = df.num_active_accounts / np.maximum(df.num_accounts, 1)

        features = ["log_balance", "tenure_months", "credit_utilization_pct", "product_breadth", "acct_ratio", "age"]
        x = df[features].astype(float)
        # PROC STDIZE METHOD=STD: (x - mean) / sample std
        std = x.std(ddof=1).replace(0, 1).fillna(1)
        xs = ((x - x.mean()) / std).fillna(0)

        k = min(5, len(df))
        km = KMeans(n_clusters=k, max_iter=50, tol=1e-3, n_init=10, random_state=seed)
        df["cluster"] = km.fit_predict(xs.to_numpy()) + 1  # FASTCLUS numbers clusters from 1

        names = ["PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL", "CREDIT_DEPENDENT", "VALUE_BASIC"]
        ordered = df.groupby("cluster")["log_balance"].mean().sort_values(ascending=False).index
        label = {cl: names[min(i, len(names) - 1)] for i, cl in enumerate(ordered)}

        # LTV/engagement use unstandardised features (the SAS join on the
        # standardised CUST_CLUSTERED dataset would yield negative scores).
        out = pd.DataFrame({
            "customer_id": df.customer_id,
            "segment_name": df.cluster.map(label),
            "segment_id": df.cluster,
            "subsegment_id": 0,
            "lifetime_value_score": (df.log_balance * df.tenure_months * df.product_breadth * 10).round(2),
            "engagement_score": (df.acct_ratio * 100).round(2),
            "digital_adoption_score": df.digital_adoption_score,
            "product_breadth_index": (df.product_breadth * 100).round(2),
            "tenure_group": df.tenure_group,
            "age_group": df.age_group,
            "balance_tier": df.balance_tier,
            "channel_preference": None,
            "cross_sell_flag": _yn((df.product_breadth < 0.50) & (df.acct_ratio >= 0.75)),
            "upsell_flag": _yn((df.balance_tier == "MODERATE") & (df.tenure_group != "NEW (<1yr)")),
            "retention_risk_flag": _yn((df.acct_ratio < 0.50) & (df.tenure_months >= 60)),
            "model_version": "SEG_V3.2",
            "effective_date": pd.Timestamp(date.today()),
            "load_ts": pd.Timestamp(datetime.now()),
        })
        _validate_frame(out, "CUSTOMER_SEGMENTS", ["customer_id"], ["customer_id", "segment_name", "segment_id"], _min_rows())
        _recreate(con, "data_products.customer_segments", PRODUCT_DDL)
        _write_df(con, "data_products.customer_segments", out)
        log.info("[01_customer_segments] segment distribution: %s", out.segment_name.value_counts().to_dict())

    return _run_logged(con, "01_customer_segments", "data_products.customer_segments", run)


def transaction_analytics(con: duckdb.DuckDBPyConnection) -> int:
    """Port of sas/02_sas_txn_analytics.sas (PROC RANK percentiles + PROC MEANS IQR anomalies)."""
    def run() -> None:
        stg = con.execute("SELECT * FROM etl_staging.stg_txn_summary").df()
        if stg.empty:
            raise PipelineStepError("02_txn_analytics: stg_txn_summary is empty")
        num = ["txn_count_total", "amt_total_debit", "amt_total_credit", "amt_total_fees", "pct_web", "pct_mobile"]
        stg[num] = stg[num].astype(float).fillna(0)
        stg["digital_txns"] = stg.txn_count_total * (stg.pct_web + stg.pct_mobile) / 100
        stg["gross_amt"] = stg.amt_total_debit + stg.amt_total_credit
        stg["is_active"] = (stg.days_since_last_txn <= 30).astype(int)
        g = stg.groupby("customer_id")
        cust = pd.DataFrame({
            "total_accounts": g.account_id.nunique(),
            "active_accounts": g.is_active.sum(),
            "total_transactions": g.txn_count_total.sum(),
            "total_debit_amt": g.amt_total_debit.sum(),
            "total_credit_amt": g.amt_total_credit.sum(),
            "total_fees": g.amt_total_fees.sum(),
            "top_spend_category": g.top_merchant_category.agg(lambda v: v.dropna().max() if v.notna().any() else None),
            "gross_amt": g.gross_amt.sum(),
            "digital_txns": g.digital_txns.sum(),
        }).reset_index()
        txns = cust.total_transactions.replace(0, np.nan)
        cust["net_cash_flow"] = cust.total_credit_amt - cust.total_debit_amt
        cust["avg_transaction_size"] = (cust.gross_amt / txns).fillna(0)
        cust["digital_txn_pct"] = (cust.digital_txns / txns * 100).fillna(0)

        cust["monthly_spend_trend"] = np.select(
            [cust.net_cash_flow > cust.avg_transaction_size * 5, cust.net_cash_flow < -cust.avg_transaction_size * 5],
            ["UP", "DOWN"], "STABLE")
        cust["fee_income"] = cust.total_fees
        cust["interest_income"] = cust.total_debit_amt * 0.02
        cust["revenue_contribution"] = cust.fee_income + cust.interest_income

        # PROC RANK GROUPS=100 (TIES=MEAN): floor(rank * 100 / (n + 1))
        ranks = cust.total_debit_amt.rank(method="average")
        cust["spend_percentile"] = np.floor(ranks * 100 / (len(cust) + 1))

        # PROC MEANS median / QRANGE; flag spend > median + 3 * IQR
        median = cust.total_debit_amt.median()
        iqr = cust.total_debit_amt.quantile(0.75) - cust.total_debit_amt.quantile(0.25)
        cust["anomaly_flag"] = _yn((cust.total_debit_amt > median + 3 * iqr) & (iqr > 0))

        cust["reporting_period"] = date.today().strftime("%Y-%m")
        cust["model_version"] = "TXN_V2.1"
        cust["effective_date"] = pd.Timestamp(date.today())
        cust["load_ts"] = pd.Timestamp(datetime.now())
        _validate_frame(cust, "TRANSACTION_ANALYTICS", ["customer_id"],
                        ["customer_id", "reporting_period", "total_transactions"], _min_rows())
        _recreate(con, "data_products.transaction_analytics", PRODUCT_DDL)
        _write_df(con, "data_products.transaction_analytics", cust)
        log.info("[02_txn_analytics] median spend=%.2f IQR=%.2f anomalies=%d",
                 median, iqr, int((cust.anomaly_flag == "Y").sum()))

    return _run_logged(con, "02_txn_analytics", "data_products.transaction_analytics", run)


def customer_risk_scores(con: duckdb.DuckDBPyConnection, seed: int = 42) -> int:
    """Port of sas/03_sas_risk_scoring.sas (PROC LOGISTIC -> LogisticRegression, weighted composite)."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    def run() -> None:
        df = con.execute("""
            SELECT r.*, c.tenure_months, c.num_active_accounts, c.total_balance, c.customer_status
            FROM etl_staging.stg_risk_factors r
            INNER JOIN etl_staging.stg_customer_360 c ON r.customer_id = c.customer_id
            WHERE c.customer_status = 'A'
        """).df()
        if df.empty:
            raise PipelineStepError("03_risk_scoring: no active customers in stg_risk_factors")
        num_cols = [c for c in df.columns if c not in ("customer_id", "customer_status", "load_ts")]
        df[num_cols] = df[num_cols].astype(float)

        df.loc[(df.external_credit_score <= 0) | df.external_credit_score.isna(), "external_credit_score"] = 680
        df["bureau_score_norm"] = (df.external_credit_score - 300) / (850 - 300) * 100
        df["balance_trend_ratio"] = np.where(df.avg_daily_balance_90d > 0,
                                             df.avg_daily_balance_30d / df.avg_daily_balance_90d.where(df.avg_daily_balance_90d > 0), 1.0)
        df["velocity_ratio"] = np.where(df.debit_velocity_30d > 0,
                                        df.debit_velocity_7d * (30 / 7) / df.debit_velocity_30d.where(df.debit_velocity_30d > 0), 1.0)
        df["default_flag"] = (df.payment_late_cnt > 2).astype(int)

        features = ["bureau_score_norm", "credit_util_ratio", "payment_ontime_pct", "balance_volatility",
                    "velocity_ratio", "account_overdraft_cnt", "large_withdrawal_cnt", "high_risk_merchant_cnt",
                    "tenure_months"]
        x = df[features].fillna(0).to_numpy()
        y = df.default_flag.to_numpy()
        if len(np.unique(y)) < 2:
            log.warning("[03_risk_scoring] DEFAULT_FLAG has a single class; using base rate as PD")
            df["prob_default"] = float(y.mean())
        else:
            model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000, random_state=seed))
            model.fit(x, y)
            df["prob_default"] = model.predict_proba(x)[:, 1]
            coefs = dict(zip(features, model[-1].coef_[0].round(3)))
            log.info("[03_risk_scoring] default rate=%.3f coefficients=%s", y.mean(), coefs)

        clip = lambda s: s.clip(lower=0, upper=100)
        df["credit_risk_component"] = clip(100 - df.bureau_score_norm)
        df["behaviour_risk_component"] = clip(100 - df.payment_ontime_pct)
        df["velocity_risk_component"] = clip((df.velocity_ratio - 1) * 50)
        df["bureau_score_component"] = clip(df.bureau_score_norm)
        df["payment_history_component"] = clip(df.payment_ontime_pct)
        df["composite_risk_score"] = (
            df.credit_risk_component * 0.30
            + df.behaviour_risk_component * 0.25
            + df.velocity_risk_component * 0.15
            + (100 - df.bureau_score_component) * 0.20
            + (100 - df.payment_history_component) * 0.10
        ).round(2)
        df["probability_of_default"] = df.prob_default.fillna(0).round(6)
        df["risk_tier"] = np.select(
            [df.composite_risk_score < 20, df.composite_risk_score < 40,
             df.composite_risk_score < 60, df.composite_risk_score < 80],
            ["LOW", "MODERATE", "ELEVATED", "HIGH"], "CRITICAL")

        # Top-two drivers, same strict ">" scan order as the SAS array loop
        comps = np.column_stack([df.credit_risk_component, df.behaviour_risk_component,
                                 df.velocity_risk_component, 100 - df.bureau_score_component])
        labels = ["CREDIT_UTILIZATION", "PAYMENT_BEHAVIOUR", "TRANSACTION_VELOCITY", "BUREAU_SCORE"]
        primary, secondary = [], []
        for row in comps:
            m1 = m2 = 0.0
            p = s = None
            for val, lbl in zip(row, labels):
                if val > m1:
                    m2, s, m1, p = m1, p, val, lbl
                elif val > m2:
                    m2, s = val, lbl
            primary.append(p)
            secondary.append(s)
        df["primary_risk_driver"] = primary
        df["secondary_risk_driver"] = secondary
        df["score_delta_30d"] = 0.0
        df["watch_list_flag"] = _yn((df.risk_tier == "CRITICAL") & (df.probability_of_default > 0.5))
        df["review_required_flag"] = _yn((df.composite_risk_score >= 60) & (df.velocity_ratio > 2.0))
        df["model_version"] = "RISK_V4.0"
        df["effective_date"] = pd.Timestamp(date.today())
        df["load_ts"] = pd.Timestamp(datetime.now())

        _validate_frame(df, "CUSTOMER_RISK_SCORES", ["customer_id"],
                        ["customer_id", "composite_risk_score", "risk_tier"], _min_rows())
        _recreate(con, "data_products.customer_risk_scores", PRODUCT_DDL)
        _write_df(con, "data_products.customer_risk_scores", df)
        log.info("[03_risk_scoring] risk tier distribution: %s", df.risk_tier.value_counts().to_dict())

    return _run_logged(con, "03_risk_scoring", "data_products.customer_risk_scores", run)


def customer_master_profile(con: duckdb.DuckDBPyConnection) -> int:
    """Port of sas/04_sas_data_products.sas (4-way MERGE keeping base customers, default handling)."""
    def run() -> None:
        base = con.execute("""
            SELECT customer_id, TRIM(first_name) || ' ' || TRIM(last_name) AS full_name, age, state_code,
                   customer_since, tenure_months, customer_status, num_accounts AS total_accounts,
                   num_active_accounts AS active_accounts, total_balance, total_credit_limit, credit_utilization_pct
            FROM etl_staging.stg_customer_360 WHERE customer_status = 'A'
        """).df()
        seg = con.execute("""
            SELECT customer_id, segment_name, lifetime_value_score, engagement_score,
                   cross_sell_flag, upsell_flag, retention_risk_flag
            FROM data_products.customer_segments
        """).df()
        txn = con.execute("""
            SELECT customer_id, total_transactions AS monthly_transactions, total_debit_amt AS monthly_spend,
                   net_cash_flow, top_spend_category, digital_txn_pct
            FROM data_products.transaction_analytics
            WHERE effective_date = (SELECT MAX(effective_date) FROM data_products.transaction_analytics)
        """).df()
        risk = con.execute("""
            SELECT customer_id, composite_risk_score, risk_tier, probability_of_default, watch_list_flag
            FROM data_products.customer_risk_scores
        """).df()

        seg["_seg"] = True
        txn["_txn"] = True
        risk["_risk"] = True
        base["_base"] = True
        merged = base
        for other in (seg, txn, risk):
            merged = merged.merge(other, on="customer_id", how="outer")
        merged = merged[merged._base.eq(True)].copy()  # SAS: `if _base;`

        no_seg = merged._seg.ne(True)
        merged.loc[no_seg, ["segment_name"]] = "UNCLASSIFIED"
        merged.loc[no_seg, ["lifetime_value_score", "engagement_score"]] = 0
        merged.loc[no_seg, ["cross_sell_flag", "upsell_flag", "retention_risk_flag"]] = "N"
        no_txn = merged._txn.ne(True)
        merged.loc[no_txn, ["monthly_transactions", "monthly_spend", "net_cash_flow", "digital_txn_pct"]] = 0
        merged.loc[no_txn, "top_spend_category"] = None
        no_risk = merged._risk.ne(True)
        merged.loc[no_risk, ["composite_risk_score", "probability_of_default"]] = np.nan
        merged.loc[no_risk, "risk_tier"] = "UNKNOWN"
        merged.loc[no_risk, "watch_list_flag"] = "N"

        merged["model_version"] = "MASTER_V1.5"
        merged["effective_date"] = pd.Timestamp(date.today())
        merged["load_ts"] = pd.Timestamp(datetime.now())
        merged = merged.sort_values("customer_id")

        _validate_frame(merged, "CUSTOMER_MASTER_PROFILE", ["customer_id"],
                        ["customer_id", "full_name", "customer_status"], _min_rows())
        _recreate(con, "data_products.customer_master_profile", PRODUCT_DDL)
        _write_df(con, "data_products.customer_master_profile", merged)
        log.info("[04_master_profile] completeness: total=%d has_segment=%d has_txn=%d has_risk=%d",
                 len(merged), int((~no_seg).sum()), int((merged.monthly_transactions > 0).sum()), int((~no_risk).sum()))

    return _run_logged(con, "04_master_profile", "data_products.customer_master_profile", run)


def phase3_python_analytics(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        "data_products.customer_segments": customer_segments(con),
        "data_products.transaction_analytics": transaction_analytics(con),
        "data_products.customer_risk_scores": customer_risk_scores(con),
        "data_products.customer_master_profile": customer_master_profile(con),
    }


# =============================================================================
# Phase 4: Post-run validation (mirrors Phase 3 of orchestration/run_full_pipeline.sh)
# =============================================================================
def phase4_validate(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Row counts for the four data product tables (0 if a table is missing)."""
    existing = {
        f"{s}.{t}" for s, t in con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'data_products'"
        ).fetchall()
    }
    counts = {t: (_count(con, t) if t in existing else 0) for t in PRODUCT_TABLES}
    for t, c in counts.items():
        log.info("[validate_row_counts] %-42s %10d rows", t.split(".")[1].upper(), c)
    return counts


# =============================================================================
# Stand-alone runner
# =============================================================================
def run_pipeline(db_path: str | os.PathLike | None = None, n_customers: int = 5000, seed: int = 42) -> dict[str, int]:
    with connect(db_path) as con:
        generate_source_data(con, n_customers=n_customers, seed=seed)
        phase2_bteq_transforms(con)
        phase3_python_analytics(con)
        counts = phase4_validate(con)
    empty = [t for t, c in counts.items() if c == 0]
    if empty:
        raise PipelineStepError(f"validate_row_counts: empty data products {empty}")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--db", default=None, help=f"DuckDB file (default: ${DB_PATH_ENV} or {DEFAULT_DB_PATH})")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    t0 = time.time()
    counts = run_pipeline(args.db, n_customers=args.customers, seed=args.seed)
    print(f"\nPipeline complete in {time.time() - t0:.1f}s -> {resolve_db_path(args.db)}")
    for t, c in counts.items():
        print(f"  {t:<42} {c:>10,}")


if __name__ == "__main__":
    main()
