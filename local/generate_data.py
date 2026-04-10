#!/usr/bin/env python3
"""
Synthetic Banking Data Generator
=================================
Generates realistic banking data for a demo migration pipeline.
Produces data matching Teradata source table schemas for:
  - CORE_BANKING_DB.CUSTOMERS
  - CORE_BANKING_DB.ACCOUNTS
  - CORE_BANKING_DB.ADDRESSES
  - TXN_PROCESSING_DB.TRANSACTION_TYPES
  - TXN_PROCESSING_DB.TRANSACTIONS
  - CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES
  - ETL_RUN_LOG

Usage:
  As a library:
      from generate_data import generate_customers, generate_accounts, ...
  As a CLI:
      python generate_data.py --output-dir ./data --customers 5000
      uv run generate_data.py --output-dir ./data --customers 5000
"""
# /// script
# requires-python = ">=3.10"
# dependencies = ["faker>=28.0", "duckdb>=1.0", "pandas>=2.0", "numpy>=1.24"]
# ///

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SEED = 42
_DEFAULT_CUSTOMERS = 5000

_ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT", "LOAN"]

_SEGMENTS = ["PREM", "PLUS", "CORE", "BASIC"]

_MERCHANT_CATEGORIES = [
    "GROCERY", "RESTAURANT", "GAS_STATION", "RETAIL", "ONLINE_SHOPPING",
    "ENTERTAINMENT", "HEALTHCARE", "TRAVEL", "UTILITIES", "INSURANCE",
    "GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP",
]

_CHANNEL_CODES = ["ATM", "POS", "WEB", "MOB", "ACH", "WIRE"]

# Transaction type weights per account type  (keys = TRANSACTION_TYPE_CD)
_TXN_WEIGHTS_BY_ACCT: dict[str, dict[str, float]] = {
    "CHECKING": {
        "DEP": 10, "WDR": 12, "POS": 25, "ACH_IN": 8, "ACH_OUT": 12,
        "ATM_WDR": 8, "WIRE_IN": 1, "WIRE_OUT": 2, "FEE_NSF": 2,
        "FEE_MON": 2, "FEE_OVD": 2, "INT_PAY": 0, "PMT": 3, "REFUND": 2,
        "CHK": 11,
    },
    "SAVINGS": {
        "DEP": 25, "WDR": 8, "POS": 0, "ACH_IN": 15, "ACH_OUT": 5,
        "ATM_WDR": 3, "WIRE_IN": 2, "WIRE_OUT": 1, "FEE_NSF": 0,
        "FEE_MON": 2, "FEE_OVD": 0, "INT_PAY": 20, "PMT": 0, "REFUND": 1,
        "CHK": 0,
    },
    "CREDIT": {
        "DEP": 0, "WDR": 0, "POS": 30, "ACH_IN": 0, "ACH_OUT": 0,
        "ATM_WDR": 0, "WIRE_IN": 0, "WIRE_OUT": 0, "FEE_NSF": 3,
        "FEE_MON": 5, "FEE_OVD": 4, "INT_PAY": 8, "PMT": 25, "REFUND": 5,
        "CHK": 0,
    },
    "LOAN": {
        "DEP": 0, "WDR": 0, "POS": 0, "ACH_IN": 0, "ACH_OUT": 0,
        "ATM_WDR": 0, "WIRE_IN": 0, "WIRE_OUT": 0, "FEE_NSF": 2,
        "FEE_MON": 5, "FEE_OVD": 3, "INT_PAY": 25, "PMT": 60, "REFUND": 0,
        "CHK": 0,
    },
}

# Channel weights per account type
_CHANNEL_WEIGHTS_BY_ACCT: dict[str, list[float]] = {
    "CHECKING": [0.10, 0.25, 0.25, 0.25, 0.10, 0.05],
    "SAVINGS":  [0.05, 0.05, 0.30, 0.35, 0.20, 0.05],
    "CREDIT":   [0.02, 0.30, 0.30, 0.30, 0.05, 0.03],
    "LOAN":     [0.00, 0.00, 0.30, 0.30, 0.35, 0.05],
}

# Amount ranges by transaction type code
_AMOUNT_RANGES: dict[str, tuple[float, float]] = {
    "DEP": (100.0, 5000.0),
    "WDR": (20.0, 2000.0),
    "POS": (5.0, 500.0),
    "ACH_IN": (200.0, 10000.0),
    "ACH_OUT": (50.0, 5000.0),
    "ATM_WDR": (20.0, 500.0),
    "WIRE_IN": (1000.0, 50000.0),
    "WIRE_OUT": (500.0, 25000.0),
    "FEE_NSF": (25.0, 39.0),
    "FEE_MON": (25.0, 39.0),
    "FEE_OVD": (25.0, 39.0),
    "INT_PAY": (0.01, 500.0),
    "PMT": (50.0, 5000.0),
    "REFUND": (5.0, 500.0),
    "CHK": (10.0, 5000.0),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
fake = Faker("en_US")
Faker.seed(_SEED)
random.seed(_SEED)
np.random.seed(_SEED)


def _progress(msg: str) -> None:
    """Print progress messages to stderr."""
    print(f"[generate_data] {msg}", file=sys.stderr, flush=True)


def _random_date(start: date, end: date) -> date:
    """Return a random date between start and end inclusive."""
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))


def _random_ts(d: date) -> datetime:
    """Given a date, return a datetime with a random time-of-day."""
    return datetime(
        d.year, d.month, d.day,
        random.randint(0, 23), random.randint(0, 59), random.randint(0, 59),
    )


def _sha256_fake() -> str:
    """Generate a realistic-looking SHA-256 hex digest."""
    return hashlib.sha256(uuid.uuid4().bytes).hexdigest()


# ---------------------------------------------------------------------------
# Generator Functions — each returns a pandas DataFrame
# ---------------------------------------------------------------------------


def generate_customers(n: int = _DEFAULT_CUSTOMERS) -> pd.DataFrame:
    """
    Generate CORE_BANKING_DB.CUSTOMERS.

    Parameters
    ----------
    n : int
        Number of customer rows to create.

    Returns
    -------
    pd.DataFrame
    """
    _progress(f"Generating {n:,} customers …")

    now = datetime.now()
    rows: list[dict] = []

    for i in range(n):
        cid = 100_001 + i
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        cust_since = _random_date(date(2005, 1, 1), date(2025, 12, 31))
        created = _random_ts(cust_since)
        updated = _random_ts(_random_date(cust_since, now.date()))

        # Status distribution: 85% A, 10% I, 5% C
        status_roll = random.random()
        if status_roll < 0.85:
            status = "A"
        elif status_roll < 0.95:
            status = "I"
        else:
            status = "C"

        rows.append({
            "CUSTOMER_ID": cid,
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "DATE_OF_BIRTH": dob,
            "SSN_HASH": _sha256_fake(),
            "EMAIL": fake.unique.email(),
            "PHONE_PRIMARY": fake.phone_number(),
            "CUSTOMER_SINCE": cust_since,
            "CUSTOMER_STATUS": status,
            "SEGMENT_CODE": random.choice(_SEGMENTS),
            "BRANCH_ID": random.randint(1, 500),
            "CREATED_TS": created,
            "UPDATED_TS": updated,
        })

        if (i + 1) % 2500 == 0:
            _progress(f"  customers: {i + 1:,}/{n:,}")

    fake.unique.clear()
    df = pd.DataFrame(rows)
    _progress(f"  customers: {len(df):,} rows generated.")
    return df


def generate_accounts(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate CORE_BANKING_DB.ACCOUNTS.

    Each customer gets 1-4 accounts with randomly selected, non-repeating types.

    Parameters
    ----------
    customers_df : pd.DataFrame
        Output of ``generate_customers``.

    Returns
    -------
    pd.DataFrame
    """
    _progress("Generating accounts …")

    now = datetime.now()
    rows: list[dict] = []
    acct_id = 200_001

    balance_ranges: dict[str, tuple[float, float]] = {
        "CHECKING": (100.0, 50_000.0),
        "SAVINGS": (500.0, 200_000.0),
        "CREDIT": (0.0, 25_000.0),
        "LOAN": (1_000.0, 500_000.0),
    }

    for _, cust in customers_df.iterrows():
        n_accts = random.choices([1, 2, 3, 4], weights=[25, 40, 25, 10])[0]
        acct_types = random.sample(_ACCOUNT_TYPES, k=n_accts)

        for atype in acct_types:
            # Status distribution: 80% O, 15% C, 5% F
            s_roll = random.random()
            if s_roll < 0.80:
                status = "O"
            elif s_roll < 0.95:
                status = "C"
            else:
                status = "F"

            cust_since = cust["CUSTOMER_SINCE"]
            if isinstance(cust_since, str):
                cust_since = date.fromisoformat(cust_since)
            open_dt = _random_date(cust_since, now.date())
            close_dt = (
                _random_date(open_dt, now.date()) if status == "C" else None
            )

            lo, hi = balance_ranges[atype]
            balance = round(random.uniform(lo, hi), 2)

            credit_limit = None
            available_balance = None
            interest_rate = None

            if atype == "CREDIT":
                credit_limit = round(random.uniform(5_000, 50_000), 2)
                available_balance = round(credit_limit - balance, 2)
                interest_rate = round(random.uniform(12.99, 24.99), 2)
            elif atype == "SAVINGS":
                available_balance = balance
                interest_rate = round(random.uniform(0.01, 4.50), 2)
            elif atype == "CHECKING":
                available_balance = balance
            elif atype == "LOAN":
                interest_rate = round(random.uniform(3.50, 15.00), 2)
                available_balance = 0.0

            created = _random_ts(open_dt)
            updated = _random_ts(_random_date(open_dt, now.date()))

            rows.append({
                "ACCOUNT_ID": acct_id,
                "CUSTOMER_ID": cust["CUSTOMER_ID"],
                "ACCOUNT_TYPE": atype,
                "ACCOUNT_STATUS": status,
                "OPEN_DATE": open_dt,
                "CLOSE_DATE": close_dt,
                "CURRENT_BALANCE": balance,
                "AVAILABLE_BALANCE": available_balance,
                "CREDIT_LIMIT": credit_limit,
                "INTEREST_RATE": interest_rate,
                "BRANCH_ID": cust["BRANCH_ID"],
                "CREATED_TS": created,
                "UPDATED_TS": updated,
            })
            acct_id += 1

    df = pd.DataFrame(rows)
    _progress(f"  accounts: {len(df):,} rows generated.")
    return df


def generate_addresses(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate CORE_BANKING_DB.ADDRESSES.

    Every customer gets a HOME address.  30 % also get MAIL, 20 % also get WORK.

    Parameters
    ----------
    customers_df : pd.DataFrame
        Output of ``generate_customers``.

    Returns
    -------
    pd.DataFrame
    """
    _progress("Generating addresses …")

    rows: list[dict] = []
    addr_id = 300_001

    for _, cust in customers_df.iterrows():
        cid = cust["CUSTOMER_ID"]
        cust_since = cust["CUSTOMER_SINCE"]
        if isinstance(cust_since, str):
            cust_since = date.fromisoformat(cust_since)

        types_to_gen = ["HOME"]
        if random.random() < 0.30:
            types_to_gen.append("MAIL")
        if random.random() < 0.20:
            types_to_gen.append("WORK")

        for atype in types_to_gen:
            eff_date = _random_date(cust_since, date.today())
            rows.append({
                "ADDRESS_ID": addr_id,
                "CUSTOMER_ID": cid,
                "ADDRESS_TYPE": atype,
                "ADDRESS_LINE1": fake.street_address(),
                "ADDRESS_LINE2": fake.secondary_address() if random.random() < 0.25 else None,
                "CITY": fake.city(),
                "STATE_CODE": fake.state_abbr(),
                "ZIP_CODE": fake.zipcode(),
                "COUNTRY_CODE": "US",
                "IS_PRIMARY": "Y" if atype == "HOME" else "N",
                "EFFECTIVE_DATE": eff_date,
                "EXPIRATION_DATE": None,  # NULL = current
            })
            addr_id += 1

    df = pd.DataFrame(rows)
    _progress(f"  addresses: {len(df):,} rows generated.")
    return df


def generate_transaction_types() -> pd.DataFrame:
    """
    Generate TXN_PROCESSING_DB.TRANSACTION_TYPES — a fixed reference table.

    Returns
    -------
    pd.DataFrame
    """
    _progress("Generating transaction_types (reference) …")

    data = [
        ("DEP",     "Deposit",              "CREDIT",   "N"),
        ("WDR",     "Withdrawal",           "DEBIT",    "N"),
        ("POS",     "Point of Sale",        "DEBIT",    "N"),
        ("ACH_IN",  "ACH Inbound",          "CREDIT",   "N"),
        ("ACH_OUT", "ACH Outbound",         "DEBIT",    "N"),
        ("ATM_WDR", "ATM Withdrawal",       "DEBIT",    "N"),
        ("WIRE_IN", "Wire Transfer In",     "CREDIT",   "N"),
        ("WIRE_OUT","Wire Transfer Out",    "DEBIT",    "N"),
        ("FEE_NSF", "NSF Fee",             "FEE",      "Y"),
        ("FEE_MON", "Monthly Fee",         "FEE",      "Y"),
        ("FEE_OVD", "Overdraft Fee",       "FEE",      "Y"),
        ("INT_PAY", "Interest Payment",    "INTEREST",  "Y"),
        ("PMT",     "Payment",             "CREDIT",    "N"),
        ("REFUND",  "Refund",              "CREDIT",    "N"),
        ("CHK",     "Check",               "DEBIT",     "N"),
    ]
    df = pd.DataFrame(data, columns=[
        "TRANSACTION_TYPE_CD", "DESCRIPTION", "CATEGORY", "IS_SYSTEM_GENERATED",
    ])
    _progress(f"  transaction_types: {len(df)} rows.")
    return df


def generate_transactions(
    accounts_df: pd.DataFrame,
    txn_types_df: pd.DataFrame,
    months: int = 12,
) -> pd.DataFrame:
    """
    Generate TXN_PROCESSING_DB.TRANSACTIONS.

    Produces ~50-200 transactions per account over the given time window.

    Parameters
    ----------
    accounts_df : pd.DataFrame
    txn_types_df : pd.DataFrame
    months : int
        How many months of history to generate.

    Returns
    -------
    pd.DataFrame
    """
    _progress(f"Generating transactions ({months} months) …")

    today = date.today()
    start_date = today - timedelta(days=months * 30)

    # Pre-build a pool of ~200 reusable merchant names
    merchant_pool = [fake.company() for _ in range(200)]

    # Pre-build lookup: txn_type_cd → category
    type_to_cat: dict[str, str] = dict(
        zip(txn_types_df["TRANSACTION_TYPE_CD"], txn_types_df["CATEGORY"])
    )

    txn_id = 500_000_001
    all_rows: list[dict] = []
    n_accts = len(accounts_df)

    for idx, (_, acct) in enumerate(accounts_df.iterrows()):
        atype = acct["ACCOUNT_TYPE"]

        # Determine number of transactions
        if atype == "CHECKING":
            n_txns = random.randint(100, 200)
        elif atype == "SAVINGS":
            n_txns = random.randint(50, 100)
        elif atype == "CREDIT":
            n_txns = random.randint(80, 180)
        else:  # LOAN
            n_txns = random.randint(12, 50)

        # Filter txn types to those with non-zero weight for this account type
        weights_map = _TXN_WEIGHTS_BY_ACCT[atype]
        valid_types = [t for t, w in weights_map.items() if w > 0]
        valid_weights = [weights_map[t] for t in valid_types]

        # Channel weights for this account type
        chan_weights = _CHANNEL_WEIGHTS_BY_ACCT[atype]

        # Generate transaction dates — weekday-heavy
        txn_dates: list[date] = []
        attempts = 0
        while len(txn_dates) < n_txns and attempts < n_txns * 5:
            d = _random_date(start_date, today)
            # 85 % chance to keep weekdays, 15 % weekends
            if d.weekday() < 5 or random.random() < 0.15:
                txn_dates.append(d)
            attempts += 1
        # Fill remaining if needed
        while len(txn_dates) < n_txns:
            txn_dates.append(_random_date(start_date, today))
        txn_dates.sort()

        running_balance = round(random.uniform(500, 10_000), 2)

        for td in txn_dates:
            txn_type_cd = random.choices(valid_types, weights=valid_weights, k=1)[0]
            cat = type_to_cat[txn_type_cd]

            lo, hi = _AMOUNT_RANGES[txn_type_cd]
            amount = round(random.uniform(lo, hi), 2)

            # Update running balance
            if cat in ("CREDIT", "INTEREST"):
                running_balance = round(running_balance + amount, 2)
            else:  # DEBIT, FEE
                running_balance = round(running_balance - amount, 2)

            # Status: 95% P, 3% R, 2% H
            sr = random.random()
            if sr < 0.95:
                status = "P"
            elif sr < 0.98:
                status = "R"
            else:
                status = "H"

            channel = random.choices(_CHANNEL_CODES, weights=chan_weights, k=1)[0]

            merchant = random.choice(merchant_pool)
            merch_cat = random.choice(_MERCHANT_CATEGORIES)

            ts = _random_ts(td)

            all_rows.append({
                "TRANSACTION_ID": txn_id,
                "ACCOUNT_ID": acct["ACCOUNT_ID"],
                "TRANSACTION_TYPE_CD": txn_type_cd,
                "TRANSACTION_DATE": td,
                "TRANSACTION_TS": ts,
                "AMOUNT": amount,
                "RUNNING_BALANCE": running_balance,
                "MERCHANT_NAME": merchant,
                "MERCHANT_CATEGORY": merch_cat,
                "CHANNEL_CODE": channel,
                "REFERENCE_NUM": str(uuid.uuid4()),
                "STATUS_CODE": status,
            })
            txn_id += 1

        if (idx + 1) % 2000 == 0:
            _progress(f"  transactions: {idx + 1:,}/{n_accts:,} accounts processed …")

    df = pd.DataFrame(all_rows)
    _progress(f"  transactions: {len(df):,} rows generated across {n_accts:,} accounts.")
    return df


def generate_bureau_scores(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES.

    Each customer gets 1-3 score records at different report dates.
    Scores are normally distributed around 680 (σ=80), clipped to [300, 850].

    Parameters
    ----------
    customers_df : pd.DataFrame
        Output of ``generate_customers``.

    Returns
    -------
    pd.DataFrame
    """
    _progress("Generating bureau scores …")

    rows: list[dict] = []
    today = date.today()

    for _, cust in customers_df.iterrows():
        cid = cust["CUSTOMER_ID"]
        n_scores = random.choices([1, 2, 3], weights=[30, 50, 20])[0]

        for _ in range(n_scores):
            score = int(np.clip(np.random.normal(680, 80), 300, 850))
            report_dt = _random_date(today - timedelta(days=730), today)
            rows.append({
                "CUSTOMER_ID": cid,
                "EXTERNAL_CREDIT_SCORE": score,
                "REPORT_DATE": report_dt,
            })

    df = pd.DataFrame(rows)
    _progress(f"  bureau_scores: {len(df):,} rows generated.")
    return df


def create_etl_run_log_table() -> pd.DataFrame:
    """
    Create an empty ETL_RUN_LOG DataFrame with the correct schema.

    Returns
    -------
    pd.DataFrame
        Empty DataFrame with columns:
        JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS
    """
    _progress("Creating empty ETL_RUN_LOG table …")
    df = pd.DataFrame({
        "JOB_NAME": pd.Series(dtype="str"),
        "STEP_NAME": pd.Series(dtype="str"),
        "STATUS": pd.Series(dtype="str"),
        "ROW_COUNT": pd.Series(dtype="int64"),
        "START_TS": pd.Series(dtype="datetime64[ns]"),
        "END_TS": pd.Series(dtype="datetime64[ns]"),
    })
    return df


# ---------------------------------------------------------------------------
# Loading helpers
# ---------------------------------------------------------------------------


def load_to_duckdb(con, n: int = _DEFAULT_CUSTOMERS, months: int = 12) -> None:
    """
    Generate all tables and register them inside an existing DuckDB connection.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An open DuckDB connection.
    n : int
        Number of customers to generate.
    months : int
        Months of transaction history.
    """
    import duckdb  # noqa: F811  (ensure available)

    _progress("Generating all data for DuckDB …")

    customers_df = generate_customers(n)
    accounts_df = generate_accounts(customers_df)
    addresses_df = generate_addresses(customers_df)
    txn_types_df = generate_transaction_types()
    transactions_df = generate_transactions(accounts_df, txn_types_df, months=months)
    bureau_scores_df = generate_bureau_scores(customers_df)
    etl_run_log_df = create_etl_run_log_table()

    table_map = {
        "customers": customers_df,
        "accounts": accounts_df,
        "addresses": addresses_df,
        "transaction_types": txn_types_df,
        "transactions": transactions_df,
        "customer_bureau_scores": bureau_scores_df,
        "etl_run_log": etl_run_log_df,
    }

    for name, df in table_map.items():
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM df")
        count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        _progress(f"  DuckDB table '{name}': {count:,} rows")

    _progress("All tables loaded into DuckDB.")


def load_to_teradata(
    host: str,
    user: str,
    password: str,
    n: int = _DEFAULT_CUSTOMERS,
    months: int = 12,
) -> None:
    """
    Generate all tables and bulk-load them into Teradata via teradatasql.

    Parameters
    ----------
    host : str
        Teradata hostname or IP.
    user : str
        Teradata username.
    password : str
        Teradata password.
    n : int
        Number of customers.
    months : int
        Months of transaction history.
    """
    try:
        import teradatasql
    except ImportError:
        _progress(
            "WARNING: teradatasql is not installed. "
            "Install it with: pip install teradatasql\n"
            "Skipping Teradata load."
        )
        return

    _progress(f"Connecting to Teradata at {host} …")
    con = teradatasql.connect(host=host, user=user, password=password)
    cur = con.cursor()

    customers_df = generate_customers(n)
    accounts_df = generate_accounts(customers_df)
    addresses_df = generate_addresses(customers_df)
    txn_types_df = generate_transaction_types()
    transactions_df = generate_transactions(accounts_df, txn_types_df, months=months)
    bureau_scores_df = generate_bureau_scores(customers_df)

    # ---- DDL for each table ------------------------------------------------
    ddl: dict[str, str] = {
        "CORE_BANKING_DB.CUSTOMERS": """
            CREATE MULTISET TABLE CORE_BANKING_DB.CUSTOMERS (
                CUSTOMER_ID   BIGINT NOT NULL,
                FIRST_NAME    VARCHAR(100),
                LAST_NAME     VARCHAR(100),
                DATE_OF_BIRTH DATE FORMAT 'YYYY-MM-DD',
                SSN_HASH      CHAR(64),
                EMAIL         VARCHAR(255),
                PHONE_PRIMARY VARCHAR(30),
                CUSTOMER_SINCE DATE FORMAT 'YYYY-MM-DD',
                CUSTOMER_STATUS CHAR(1),
                SEGMENT_CODE  VARCHAR(10),
                BRANCH_ID     INTEGER,
                CREATED_TS    TIMESTAMP(6),
                UPDATED_TS    TIMESTAMP(6)
            ) PRIMARY INDEX (CUSTOMER_ID)
        """,
        "CORE_BANKING_DB.ACCOUNTS": """
            CREATE MULTISET TABLE CORE_BANKING_DB.ACCOUNTS (
                ACCOUNT_ID       BIGINT NOT NULL,
                CUSTOMER_ID      BIGINT,
                ACCOUNT_TYPE     VARCHAR(20),
                ACCOUNT_STATUS   CHAR(1),
                OPEN_DATE        DATE FORMAT 'YYYY-MM-DD',
                CLOSE_DATE       DATE FORMAT 'YYYY-MM-DD',
                CURRENT_BALANCE  DECIMAL(15,2),
                AVAILABLE_BALANCE DECIMAL(15,2),
                CREDIT_LIMIT     DECIMAL(15,2),
                INTEREST_RATE    DECIMAL(5,2),
                BRANCH_ID        INTEGER,
                CREATED_TS       TIMESTAMP(6),
                UPDATED_TS       TIMESTAMP(6)
            ) PRIMARY INDEX (ACCOUNT_ID)
        """,
        "CORE_BANKING_DB.ADDRESSES": """
            CREATE MULTISET TABLE CORE_BANKING_DB.ADDRESSES (
                ADDRESS_ID     BIGINT NOT NULL,
                CUSTOMER_ID    BIGINT,
                ADDRESS_TYPE   VARCHAR(10),
                ADDRESS_LINE1  VARCHAR(255),
                ADDRESS_LINE2  VARCHAR(255),
                CITY           VARCHAR(100),
                STATE_CODE     CHAR(2),
                ZIP_CODE       VARCHAR(10),
                COUNTRY_CODE   CHAR(2),
                IS_PRIMARY     CHAR(1),
                EFFECTIVE_DATE DATE FORMAT 'YYYY-MM-DD',
                EXPIRATION_DATE DATE FORMAT 'YYYY-MM-DD'
            ) PRIMARY INDEX (ADDRESS_ID)
        """,
        "TXN_PROCESSING_DB.TRANSACTION_TYPES": """
            CREATE MULTISET TABLE TXN_PROCESSING_DB.TRANSACTION_TYPES (
                TRANSACTION_TYPE_CD VARCHAR(10) NOT NULL,
                DESCRIPTION         VARCHAR(100),
                CATEGORY            VARCHAR(20),
                IS_SYSTEM_GENERATED CHAR(1)
            ) PRIMARY INDEX (TRANSACTION_TYPE_CD)
        """,
        "TXN_PROCESSING_DB.TRANSACTIONS": """
            CREATE MULTISET TABLE TXN_PROCESSING_DB.TRANSACTIONS (
                TRANSACTION_ID     BIGINT NOT NULL,
                ACCOUNT_ID         BIGINT,
                TRANSACTION_TYPE_CD VARCHAR(10),
                TRANSACTION_DATE   DATE FORMAT 'YYYY-MM-DD',
                TRANSACTION_TS     TIMESTAMP(6),
                AMOUNT             DECIMAL(15,2),
                RUNNING_BALANCE    DECIMAL(15,2),
                MERCHANT_NAME      VARCHAR(255),
                MERCHANT_CATEGORY  VARCHAR(50),
                CHANNEL_CODE       VARCHAR(10),
                REFERENCE_NUM      VARCHAR(50),
                STATUS_CODE        CHAR(1)
            ) PRIMARY INDEX (TRANSACTION_ID)
        """,
        "CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES": """
            CREATE MULTISET TABLE CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES (
                CUSTOMER_ID           BIGINT,
                EXTERNAL_CREDIT_SCORE INTEGER,
                REPORT_DATE           DATE FORMAT 'YYYY-MM-DD'
            ) PRIMARY INDEX (CUSTOMER_ID)
        """,
        "ETL_RUN_LOG": """
            CREATE MULTISET TABLE ETL_RUN_LOG (
                JOB_NAME   VARCHAR(100),
                STEP_NAME  VARCHAR(100),
                STATUS     VARCHAR(20),
                ROW_COUNT  INTEGER,
                START_TS   TIMESTAMP(6),
                END_TS     TIMESTAMP(6)
            ) PRIMARY INDEX (JOB_NAME)
        """,
    }

    table_data: dict[str, pd.DataFrame] = {
        "CORE_BANKING_DB.CUSTOMERS": customers_df,
        "CORE_BANKING_DB.ACCOUNTS": accounts_df,
        "CORE_BANKING_DB.ADDRESSES": addresses_df,
        "TXN_PROCESSING_DB.TRANSACTION_TYPES": txn_types_df,
        "TXN_PROCESSING_DB.TRANSACTIONS": transactions_df,
        "CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES": bureau_scores_df,
    }

    try:
        for table_name, create_sql in ddl.items():
            _progress(f"  Teradata: Creating {table_name} …")
            try:
                cur.execute(f"DROP TABLE {table_name}")
            except Exception:
                pass
            cur.execute(create_sql)

        for table_name, df in table_data.items():
            if df.empty:
                continue
            cols = ", ".join(df.columns)
            placeholders = ", ".join(["?"] * len(df.columns))
            insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            _progress(f"  Teradata: Loading {len(df):,} rows into {table_name} …")

            # Batch insert in chunks of 10 000
            chunk_size = 10_000
            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start: start + chunk_size]
                rows = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
                cur.executemany(insert_sql, rows)

            _progress(f"  Teradata: {table_name} loaded.")

        con.commit()
        _progress("All tables loaded into Teradata.")
    except Exception as exc:
        _progress(f"ERROR loading to Teradata: {exc}")
        raise
    finally:
        cur.close()
        con.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _write_csvs(output_dir: str, n: int, months: int = 12) -> None:
    """Generate all data and write CSV files to *output_dir*."""
    os.makedirs(output_dir, exist_ok=True)

    customers_df = generate_customers(n)
    accounts_df = generate_accounts(customers_df)
    addresses_df = generate_addresses(customers_df)
    txn_types_df = generate_transaction_types()
    transactions_df = generate_transactions(accounts_df, txn_types_df, months=months)
    bureau_scores_df = generate_bureau_scores(customers_df)
    etl_run_log_df = create_etl_run_log_table()

    files: dict[str, pd.DataFrame] = {
        "customers.csv": customers_df,
        "accounts.csv": accounts_df,
        "addresses.csv": addresses_df,
        "transaction_types.csv": txn_types_df,
        "transactions.csv": transactions_df,
        "customer_bureau_scores.csv": bureau_scores_df,
        "etl_run_log.csv": etl_run_log_df,
    }

    for fname, df in files.items():
        path = os.path.join(output_dir, fname)
        df.to_csv(path, index=False)
        _progress(f"  Wrote {path}  ({len(df):,} rows)")

    _progress("Done — all CSV files written.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic banking data for demo pipelines.",
    )
    parser.add_argument(
        "--output-dir",
        default="./data",
        help="Directory to write CSV files (default: ./data)",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=_DEFAULT_CUSTOMERS,
        help=f"Number of customers to generate (default: {_DEFAULT_CUSTOMERS})",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=12,
        help="Months of transaction history (default: 12)",
    )
    args = parser.parse_args()
    _write_csvs(args.output_dir, args.customers, args.months)
