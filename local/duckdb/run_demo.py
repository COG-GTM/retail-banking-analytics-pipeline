# /// script
# requires-python = ">=3.10"
# dependencies = ["faker>=28.0", "duckdb>=1.0", "pandas>=2.0", "scikit-learn>=1.3", "numpy>=1.24"]
# ///
"""
Modern Python/DuckDB pipeline engine for the Retail Banking Analytics demo.

This module is the local, dependency-free (no Teradata, no SAS) execution engine
for the pipeline.  It faithfully reproduces the legacy ETL logic:

    Phase 1 - Source generation        : _builtin_populate_sources
    Phase 2 - BTEQ staging transforms  : phase2_bteq_transforms   (DuckDB SQL)
              <- bteq/01_stg_customer_360.bteq
              <- bteq/02_stg_txn_summary.bteq
              <- bteq/03_stg_risk_factors.bteq
    Phase 3 - SAS analytics            : phase3_python_analytics  (pandas + scikit-learn)
              <- sas/01_sas_customer_segments.sas   (k-means segmentation)
              <- sas/02_sas_txn_analytics.sas       (percentile rank + IQR anomaly)
              <- sas/03_sas_risk_scoring.sas        (logistic-regression risk scoring)
              <- sas/04_sas_data_products.sas       (golden-record merge)

`export_data.py` drives the three public functions in order.  The schemas of the
produced tables follow ddl/01_staging_tables.sql and ddl/02_data_product_tables.sql.

The pipeline is deterministic (fixed RNG seeds) apart from wall-clock LOAD_TS
columns, so repeated runs yield reproducible analytics.
"""
from __future__ import annotations

import random
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
from faker import Faker

SEED = 42

# Reference data ---------------------------------------------------------------
TRANSACTION_TYPES = [
    # cd,   description,        category,   is_revenue
    ("PUR", "Purchase",          "DEBIT",   "N"),
    ("WDR", "Withdrawal",        "DEBIT",   "N"),
    ("ATM", "ATM Withdrawal",    "DEBIT",   "N"),
    ("ACH", "ACH Debit",         "DEBIT",   "N"),
    ("DEP", "Deposit",           "CREDIT",  "N"),
    ("PMT", "Loan/Card Payment", "CREDIT",  "N"),
    ("XFR", "Transfer In",       "CREDIT",  "N"),
    ("INT", "Interest Credit",   "CREDIT",  "Y"),
    ("FEE", "Service Fee",       "FEE",     "Y"),
    ("NSF", "NSF Fee",           "FEE",     "Y"),
    ("ODF", "Overdraft Fee",     "FEE",     "Y"),
]
DEBIT_TYPES = [t[0] for t in TRANSACTION_TYPES if t[2] == "DEBIT"]
CREDIT_TYPES = [t[0] for t in TRANSACTION_TYPES if t[2] == "CREDIT"]
FEE_TYPES = [t[0] for t in TRANSACTION_TYPES if t[2] == "FEE"]

ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT", "LOAN"]
SEGMENT_CODES = ["PREMIER", "DIGITAL", "MASS", "WEALTH", "STUDENT", "SMALL_BIZ"]
CHANNELS = ["ATM", "POS", "WEB", "MOB", "ACH", "WIRE", "INTL"]
CHANNEL_P = [0.18, 0.30, 0.20, 0.18, 0.08, 0.04, 0.02]
MERCHANT_CATEGORIES = [
    "GROCERY", "GAS_STATION", "RESTAURANT", "TRAVEL", "HEALTHCARE", "RETAIL",
    "UTILITIES", "ENTERTAINMENT", "EDUCATION", "INSURANCE",
    "GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP",
]
MERCHANT_CATEGORY_P = [
    0.18, 0.12, 0.13, 0.08, 0.08, 0.12, 0.07, 0.06, 0.04, 0.05,
    0.01, 0.006, 0.008, 0.006,
]


def _seed_everything() -> None:
    Faker.seed(SEED)
    random.seed(SEED)
    np.random.seed(SEED)


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    """Insert a DataFrame into an existing DuckDB table (columns cast to table types)."""
    con.register("_load_df", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM _load_df")
    con.unregister("_load_df")


# =============================================================================
# Phase 1 - Source data generation
# =============================================================================
def _builtin_populate_sources(con: duckdb.DuckDBPyConnection, n_customers: int = 5000) -> None:
    """Populate the six source tables with realistic synthetic banking data.

    The tables are expected to already exist (created by export_data.py) in the
    core_banking / txn_processing schemas.
    """
    _seed_everything()
    fake = Faker()
    now = datetime.now()
    today = pd.Timestamp.now().normalize()

    # ---- transaction_types (reference) --------------------------------------
    tt_df = pd.DataFrame(
        [(cd, desc, cat, rev, pd.Timestamp("2020-01-01"), pd.NaT)
         for cd, desc, cat, rev in TRANSACTION_TYPES],
        columns=["transaction_type_cd", "description", "category", "is_revenue",
                 "effective_date", "expiration_date"],
    )
    _insert_df(con, "txn_processing.transaction_types", tt_df)

    # ---- customers ----------------------------------------------------------
    statuses = np.random.choice(["A", "I", "C"], size=n_customers, p=[0.82, 0.13, 0.05])
    cust_rows = []
    for i in range(1, n_customers + 1):
        dob = fake.date_between(start_date="-85y", end_date="-18y")
        since = fake.date_between(start_date="-15y", end_date="-30d")
        cust_rows.append((
            i, fake.first_name(), fake.last_name(), dob,
            f"sha256_{i - 1:06d}", f"user{i - 1}@example.com",
            f"555-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            since, statuses[i - 1], random.choice(SEGMENT_CODES),
            random.randint(1, 500), now, now,
        ))
    customers_df = pd.DataFrame(cust_rows, columns=[
        "customer_id", "first_name", "last_name", "date_of_birth", "ssn_hash",
        "email", "phone_primary", "customer_since", "customer_status",
        "segment_code", "branch_id", "created_ts", "updated_ts"])
    for col in ("date_of_birth", "customer_since"):
        customers_df[col] = pd.to_datetime(customers_df[col])
    _insert_df(con, "core_banking.customers", customers_df)

    # ---- accounts -----------------------------------------------------------
    acct_rows = []
    acct_id = 0
    account_index: list[tuple[int, int, str, str]] = []  # (account_id, customer_id, type, open_date)
    for cust in cust_rows:
        cid = cust[0]
        n_acct = random.randint(1, 4)
        types = random.sample(ACCOUNT_TYPES, k=min(n_acct, len(ACCOUNT_TYPES)))
        for atype in types:
            acct_id += 1
            open_date = fake.date_between(start_date="-12y", end_date="-30d")
            status = np.random.choice(["O", "C", "F"], p=[0.9, 0.07, 0.03])
            close_date = (fake.date_between(start_date=open_date, end_date="today")
                          if status == "C" else None)
            if atype == "CREDIT":
                credit_limit = float(random.choice([2000, 5000, 10000, 15000, 25000]))
                current_balance = round(random.uniform(0, credit_limit), 2)
                available_balance = round(credit_limit - current_balance, 2)
            elif atype == "LOAN":
                credit_limit = None
                current_balance = round(random.uniform(1000, 60000), 2)
                available_balance = 0.0
            else:
                credit_limit = None
                current_balance = round(random.uniform(-500, 80000), 2)
                available_balance = round(current_balance * 0.9, 2)
            acct_rows.append((
                acct_id, cid, atype, status, open_date, close_date,
                current_balance, available_balance, credit_limit,
                round(random.uniform(0.001, 0.2499), 4), random.randint(1, 500), now, now,
            ))
            account_index.append((acct_id, cid, atype, open_date))
    accounts_df = pd.DataFrame(acct_rows, columns=[
        "account_id", "customer_id", "account_type", "account_status", "open_date",
        "close_date", "current_balance", "available_balance", "credit_limit",
        "interest_rate", "branch_id", "created_ts", "updated_ts"])
    for col in ("open_date", "close_date"):
        accounts_df[col] = pd.to_datetime(accounts_df[col])
    _insert_df(con, "core_banking.accounts", accounts_df)

    # ---- addresses ----------------------------------------------------------
    addr_rows = []
    addr_id = 0
    for cust in cust_rows:
        cid = cust[0]
        # Primary HOME address (no line 2 so PRIMARY_ADDRESS has no trailing comma)
        addr_id += 1
        addr_rows.append((
            addr_id, cid, "HOME", fake.street_address(), None, fake.city(),
            fake.state_abbr(), fake.postcode()[:10], "US", "Y",
            fake.date_between(start_date="-6y", end_date="today"), None, now, now,
        ))
        if random.random() < 0.4:
            addr_id += 1
            addr_rows.append((
                addr_id, cid, "MAIL", fake.street_address(),
                f"Apt {random.randint(1, 400)}", fake.city(), fake.state_abbr(),
                fake.postcode()[:10], "US", "N",
                fake.date_between(start_date="-6y", end_date="today"), None, now, now,
            ))
    addresses_df = pd.DataFrame(addr_rows, columns=[
        "address_id", "customer_id", "address_type", "address_line_1",
        "address_line_2", "city", "state_code", "zip_code", "country_code",
        "is_primary", "effective_date", "expiration_date", "created_ts", "updated_ts"])
    for col in ("effective_date", "expiration_date"):
        addresses_df[col] = pd.to_datetime(addresses_df[col])
    _insert_df(con, "core_banking.addresses", addresses_df)

    # ---- customer_bureau_scores --------------------------------------------
    bureau_rows = [(
        cid := cust[0],
        int(np.clip(np.random.normal(710, 70), 300, 850)),
        fake.date_between(start_date="-6m", end_date="today"),
    ) for cust in cust_rows]
    bureau_df = pd.DataFrame(bureau_rows, columns=[
        "customer_id", "external_credit_score", "report_date"])
    bureau_df["report_date"] = pd.to_datetime(bureau_df["report_date"])
    _insert_df(con, "core_banking.customer_bureau_scores", bureau_df)

    # ---- transactions (vectorised for speed) --------------------------------
    merchant_pool = [fake.company() for _ in range(max(40, n_customers // 5))]
    txn_frames = []
    next_txn_id = 1
    for acc_id, cid, atype, open_date in account_index:
        n_txn = random.randint(20, 90)
        # Bias categories per account type
        if atype in ("CREDIT", "LOAN"):
            type_codes = np.random.choice(
                DEBIT_TYPES + CREDIT_TYPES + FEE_TYPES, size=n_txn,
                p=_norm([0.18, 0.10, 0.04, 0.06, 0.10, 0.34, 0.04, 0.02, 0.04, 0.02, 0.02]))
        else:
            type_codes = np.random.choice(
                DEBIT_TYPES + CREDIT_TYPES + FEE_TYPES, size=n_txn,
                p=_norm([0.32, 0.14, 0.10, 0.08, 0.18, 0.04, 0.04, 0.02, 0.03, 0.02, 0.03]))
        cat_map = {cd: cat for cd, _, cat, _ in TRANSACTION_TYPES}
        cats = np.array([cat_map[c] for c in type_codes])

        days_ago = np.random.randint(0, 425, size=n_txn)
        txn_dates = today - pd.to_timedelta(days_ago, unit="D")
        secs = np.random.randint(0, 86400, size=n_txn)
        txn_ts = txn_dates + pd.to_timedelta(secs, unit="s")

        base = np.random.gamma(2.0, 60.0, size=n_txn) + 5.0
        large = np.random.random(n_txn) < 0.03
        base[large] += np.random.uniform(5000, 12000, size=large.sum())
        fee_amt = np.random.uniform(10, 45, size=n_txn)
        amount = np.where(cats == "CREDIT", base,
                          np.where(cats == "FEE", -fee_amt, -base))
        amount = np.round(amount, 2)

        merch_names = np.random.choice(merchant_pool, size=n_txn).astype(object)
        no_merchant = (cats != "DEBIT") | (np.random.random(n_txn) < 0.05)
        merch_names[no_merchant] = None
        merch_cats = np.random.choice(MERCHANT_CATEGORIES, size=n_txn, p=_norm(MERCHANT_CATEGORY_P)).astype(object)
        merch_cats[no_merchant] = None
        channels = np.random.choice(CHANNELS, size=n_txn, p=_norm(CHANNEL_P))
        status = np.random.choice(["P", "H", "R"], size=n_txn, p=[0.93, 0.05, 0.02])

        df = pd.DataFrame({
            "transaction_id": np.arange(next_txn_id, next_txn_id + n_txn),
            "account_id": acc_id,
            "transaction_type_cd": type_codes,
            "transaction_date": txn_dates,
            "transaction_ts": txn_ts,
            "amount": amount,
            "merchant_name": merch_names,
            "merchant_category": merch_cats,
            "channel_code": channels,
            "reference_num": [f"REF{n:08d}" for n in range(next_txn_id - 1, next_txn_id - 1 + n_txn)],
            "status_code": status,
        })
        next_txn_id += n_txn
        # Running balance = starting balance + cumulative amount (chronological)
        df = df.sort_values("transaction_ts")
        start_bal = random.uniform(-200, 15000)
        df["running_balance"] = np.round(start_bal + df["amount"].cumsum(), 2)
        txn_frames.append(df)

    transactions_df = pd.concat(txn_frames, ignore_index=True)
    transactions_df["created_ts"] = now
    transactions_df = transactions_df[[
        "transaction_id", "account_id", "transaction_type_cd", "transaction_date",
        "transaction_ts", "amount", "running_balance", "merchant_name",
        "merchant_category", "channel_code", "reference_num", "status_code", "created_ts"]]
    _insert_df(con, "txn_processing.transactions", transactions_df)


def _norm(weights: list[float]) -> np.ndarray:
    arr = np.asarray(weights, dtype=float)
    return arr / arr.sum()


# =============================================================================
# Phase 2 - BTEQ staging transforms (DuckDB SQL)
# =============================================================================
def phase2_bteq_transforms(con: duckdb.DuckDBPyConnection) -> None:
    """Reproduce the three BTEQ staging scripts as DuckDB SQL."""
    con.execute("CREATE SCHEMA IF NOT EXISTS etl_staging")
    _stg_customer_360(con)
    _stg_txn_summary(con)
    _stg_risk_factors(con)


def _stg_customer_360(con: duckdb.DuckDBPyConnection) -> None:
    """bteq/01_stg_customer_360.bteq"""
    con.execute("""
        CREATE OR REPLACE TABLE etl_staging.stg_customer_360 AS
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.date_of_birth,
            CAST((current_date - c.date_of_birth) / 365.25 AS SMALLINT)            AS age,
            c.customer_since,
            CAST(date_diff('month', c.customer_since, current_date) AS INTEGER)    AS tenure_months,
            c.customer_status,
            c.segment_code,
            c.branch_id,
            trim(a.address_line_1) || COALESCE(', ' || trim(a.address_line_2), '') AS primary_address,
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
            CASE
                WHEN acct_agg.total_credit_limit > 0
                THEN CAST(acct_agg.credit_balance / acct_agg.total_credit_limit * 100 AS DECIMAL(5,2))
                ELSE 0.00
            END                                                                   AS credit_utilization_pct,
            now()                                                                 AS load_ts
        FROM core_banking.customers c
        LEFT JOIN (
            SELECT customer_id, address_line_1, address_line_2, city, state_code, zip_code
            FROM core_banking.addresses
            WHERE address_type = 'HOME'
              AND (expiration_date IS NULL OR expiration_date > current_date)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_date DESC) = 1
        ) a ON c.customer_id = a.customer_id
        LEFT JOIN (
            SELECT
                customer_id,
                COUNT(*)                                                       AS num_accounts,
                SUM(CASE WHEN account_status = 'O' THEN 1 ELSE 0 END)          AS num_active_accounts,
                MAX(CASE WHEN account_type = 'CHECKING' THEN 'Y' ELSE 'N' END) AS has_checking,
                MAX(CASE WHEN account_type = 'SAVINGS'  THEN 'Y' ELSE 'N' END) AS has_savings,
                MAX(CASE WHEN account_type = 'CREDIT'   THEN 'Y' ELSE 'N' END) AS has_credit,
                MAX(CASE WHEN account_type = 'LOAN'     THEN 'Y' ELSE 'N' END) AS has_loan,
                SUM(COALESCE(current_balance, 0))                              AS total_balance,
                SUM(CASE WHEN account_type = 'CREDIT' THEN COALESCE(credit_limit, 0) ELSE 0 END)    AS total_credit_limit,
                SUM(CASE WHEN account_type = 'CREDIT' THEN COALESCE(current_balance, 0) ELSE 0 END) AS credit_balance
            FROM core_banking.accounts
            GROUP BY customer_id
        ) acct_agg ON c.customer_id = acct_agg.customer_id
        WHERE c.customer_status IN ('A', 'I')
    """)


def _stg_txn_summary(con: duckdb.DuckDBPyConnection) -> None:
    """bteq/02_stg_txn_summary.bteq (LOOKBACK_MONTHS = 12)"""
    con.execute("""
        CREATE OR REPLACE TABLE etl_staging.stg_txn_summary AS
        WITH params AS (
            SELECT (current_date - INTERVAL 12 MONTH)::DATE AS period_start,
                   current_date AS period_end
        ),
        top_cat AS (
            SELECT account_id, merchant_category
            FROM (
                SELECT t2.account_id, t2.merchant_category, SUM(ABS(t2.amount)) AS cat_spend
                FROM txn_processing.transactions t2, params rp2
                WHERE t2.transaction_date BETWEEN rp2.period_start AND rp2.period_end
                  AND t2.status_code = 'P'
                  AND t2.merchant_category IS NOT NULL
                GROUP BY t2.account_id, t2.merchant_category
            )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY cat_spend DESC) = 1
        )
        SELECT
            acct.customer_id,
            acct.account_id,
            acct.account_type,
            rp.period_start AS summary_period_start,
            rp.period_end   AS summary_period_end,
            COUNT(*)                                                            AS txn_count_total,
            SUM(CASE WHEN tt.category = 'DEBIT'  THEN 1 ELSE 0 END)             AS txn_count_debit,
            SUM(CASE WHEN tt.category = 'CREDIT' THEN 1 ELSE 0 END)             AS txn_count_credit,
            SUM(CASE WHEN tt.category = 'FEE'    THEN 1 ELSE 0 END)             AS txn_count_fee,
            SUM(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_total_debit,
            SUM(CASE WHEN tt.category = 'CREDIT' THEN t.amount ELSE 0 END)      AS amt_total_credit,
            SUM(CASE WHEN tt.category = 'FEE'    THEN ABS(t.amount) ELSE 0 END) AS amt_total_fees,
            AVG(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) END)        AS amt_avg_debit,
            AVG(CASE WHEN tt.category = 'CREDIT' THEN t.amount END)             AS amt_avg_credit,
            MAX(CASE WHEN tt.category = 'DEBIT'  THEN ABS(t.amount) ELSE 0 END) AS amt_max_single_debit,
            MAX(CASE WHEN tt.category = 'CREDIT' THEN t.amount ELSE 0 END)      AS amt_max_single_credit,
            COUNT(DISTINCT t.merchant_name)                                     AS distinct_merchants,
            MAX(top_cat.merchant_category)                                      AS top_merchant_category,
            CAST(SUM(CASE WHEN t.channel_code = 'ATM' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_atm,
            CAST(SUM(CASE WHEN t.channel_code = 'POS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_pos,
            CAST(SUM(CASE WHEN t.channel_code = 'WEB' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_web,
            CAST(SUM(CASE WHEN t.channel_code = 'MOB' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct_mobile,
            CAST(current_date - MAX(t.transaction_date) AS INTEGER)            AS days_since_last_txn,
            now()                                                              AS load_ts
        FROM txn_processing.transactions t
        JOIN core_banking.accounts acct ON t.account_id = acct.account_id
        JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
        CROSS JOIN params rp
        LEFT JOIN top_cat ON t.account_id = top_cat.account_id
        WHERE t.transaction_date BETWEEN rp.period_start AND rp.period_end
          AND t.status_code = 'P'
        GROUP BY acct.customer_id, acct.account_id, acct.account_type,
                 rp.period_start, rp.period_end, top_cat.merchant_category
    """)


def _stg_risk_factors(con: duckdb.DuckDBPyConnection) -> None:
    """bteq/03_stg_risk_factors.bteq (work tables -> assembled risk feature vector)"""
    con.execute("""
        CREATE OR REPLACE TABLE etl_staging.stg_risk_factors AS
        WITH daily_balance AS (  -- WRK_DAILY_BALANCE: last txn per account/day (3m)
            SELECT acct.customer_id, t.account_id, t.transaction_date, t.running_balance AS eod_balance
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            WHERE t.transaction_date >= (current_date - INTERVAL 3 MONTH)
              AND t.status_code = 'P'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.account_id, t.transaction_date ORDER BY t.transaction_ts DESC) = 1
        ),
        pmt_acct AS (  -- WRK_PAYMENT_HISTORY: payments on credit/loan accounts (24m)
            SELECT
                acct.customer_id,
                acct.account_id,
                COUNT(*) AS total_payments,
                SUM(CASE WHEN t.transaction_date
                        <= acct.open_date + to_months(CAST(date_diff('month', acct.open_date, t.transaction_date) AS INTEGER) + 1)
                    THEN 1 ELSE 0 END) AS ontime_payments,
                SUM(CASE WHEN t.transaction_date
                        > acct.open_date + to_months(CAST(date_diff('month', acct.open_date, t.transaction_date) AS INTEGER) + 1)
                    THEN 1 ELSE 0 END) AS late_payments,
                CAST(date_diff('month',
                    COALESCE(MAX(CASE WHEN t.transaction_date
                            > acct.open_date + to_months(CAST(date_diff('month', acct.open_date, t.transaction_date) AS INTEGER) + 1)
                        THEN t.transaction_date END), acct.open_date),
                    current_date) AS INTEGER) AS months_since_last_late
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
            WHERE acct.account_type IN ('CREDIT', 'LOAN')
              AND tt.category = 'CREDIT'
              AND t.status_code = 'P'
              AND t.transaction_date >= (current_date - INTERVAL 24 MONTH)
            GROUP BY acct.customer_id, acct.account_id, acct.open_date
        ),
        overdraft AS (
            SELECT acct.customer_id,
                   SUM(CASE WHEN t.running_balance < 0 THEN 1 ELSE 0 END) AS overdraft_count,
                   SUM(CASE WHEN tt.category = 'FEE' AND tt.description LIKE '%NSF%'
                            THEN ABS(t.amount) ELSE 0 END) AS nsf_total
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
            WHERE t.transaction_date >= (current_date - INTERVAL 12 MONTH) AND t.status_code = 'P'
            GROUP BY acct.customer_id
        ),
        lg_wd AS (
            SELECT acct.customer_id, COUNT(*) AS large_wd_cnt, SUM(ABS(t.amount)) AS large_wd_amt
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
            WHERE tt.category = 'DEBIT' AND ABS(t.amount) >= 5000
              AND t.transaction_date >= (current_date - INTERVAL 12 MONTH) AND t.status_code = 'P'
            GROUP BY acct.customer_id
        ),
        bal AS (
            SELECT customer_id,
                   AVG(CASE WHEN transaction_date >= current_date - 30 THEN eod_balance END) AS avg_bal_30d,
                   AVG(CASE WHEN transaction_date >= current_date - 90 THEN eod_balance END) AS avg_bal_90d,
                   STDDEV_POP(eod_balance) AS bal_stddev
            FROM daily_balance
            GROUP BY customer_id
        ),
        credit AS (
            SELECT customer_id,
                   SUM(COALESCE(current_balance, 0)) AS total_credit_bal,
                   SUM(COALESCE(credit_limit, 0))    AS total_credit_limit
            FROM core_banking.accounts
            WHERE account_type = 'CREDIT' AND account_status = 'O'
            GROUP BY customer_id
        ),
        pmh AS (
            SELECT customer_id,
                   SUM(total_payments)  AS total_payments,
                   SUM(ontime_payments) AS ontime_payments,
                   SUM(late_payments)   AS late_payments,
                   MIN(months_since_last_late) AS months_since_last_late
            FROM pmt_acct GROUP BY customer_id
        ),
        bureau AS (
            SELECT customer_id, external_credit_score AS credit_score
            FROM core_banking.customer_bureau_scores
            QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY report_date DESC) = 1
        ),
        vel AS (
            SELECT acct.customer_id,
                   SUM(CASE WHEN t.transaction_date >= current_date - 7  THEN ABS(t.amount) ELSE 0 END) AS debit_7d,
                   SUM(CASE WHEN t.transaction_date >= current_date - 30 THEN ABS(t.amount) ELSE 0 END) AS debit_30d
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            JOIN txn_processing.transaction_types tt ON t.transaction_type_cd = tt.transaction_type_cd
            WHERE tt.category = 'DEBIT' AND t.transaction_date >= current_date - 30 AND t.status_code = 'P'
            GROUP BY acct.customer_id
        ),
        recent_merch AS (
            SELECT DISTINCT acct.customer_id, t.account_id, t.merchant_name
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            WHERE t.transaction_date >= current_date - 30 AND t.status_code = 'P'
              AND t.merchant_name IS NOT NULL
        ),
        prior_merch AS (
            SELECT DISTINCT account_id, merchant_name
            FROM txn_processing.transactions
            WHERE transaction_date < current_date - 30 AND merchant_name IS NOT NULL
        ),
        new_merch AS (
            SELECT r.customer_id, COUNT(DISTINCT r.merchant_name) AS new_merch_30d
            FROM recent_merch r
            LEFT JOIN prior_merch p ON r.account_id = p.account_id AND r.merchant_name = p.merchant_name
            WHERE p.merchant_name IS NULL
            GROUP BY r.customer_id
        ),
        merch_risk AS (
            SELECT acct.customer_id,
                   SUM(CASE WHEN t.channel_code = 'INTL' THEN 1 ELSE 0 END) AS intl_txn_cnt,
                   SUM(CASE WHEN t.merchant_category IN
                        ('GAMBLING', 'WIRE_TRANSFER_INTL', 'CRYPTO_EXCHANGE', 'PAWN_SHOP')
                       THEN 1 ELSE 0 END) AS high_risk_cnt
            FROM txn_processing.transactions t
            JOIN core_banking.accounts acct ON t.account_id = acct.account_id
            WHERE t.transaction_date >= (current_date - INTERVAL 6 MONTH) AND t.status_code = 'P'
            GROUP BY acct.customer_id
        )
        SELECT
            c.customer_id,
            COALESCE(overdraft.overdraft_count, 0)                       AS account_overdraft_cnt,
            COALESCE(overdraft.nsf_total, 0.00)                          AS nsf_fee_total,
            COALESCE(lg_wd.large_wd_cnt, 0)                              AS large_withdrawal_cnt,
            COALESCE(lg_wd.large_wd_amt, 0.00)                           AS large_withdrawal_amt,
            COALESCE(bal.avg_bal_30d, 0.00)                             AS avg_daily_balance_30d,
            COALESCE(bal.avg_bal_90d, 0.00)                             AS avg_daily_balance_90d,
            COALESCE(bal.bal_stddev, 0.0000)                            AS balance_volatility,
            CASE WHEN credit.total_credit_limit > 0
                 THEN CAST(credit.total_credit_bal / credit.total_credit_limit AS DECIMAL(5,4))
                 ELSE 0.0000 END                                        AS credit_util_ratio,
            CASE WHEN pmh.total_payments > 0
                 THEN CAST(pmh.ontime_payments * 100.0 / pmh.total_payments AS DECIMAL(5,2))
                 ELSE 100.00 END                                        AS payment_ontime_pct,
            COALESCE(pmh.late_payments, 0)                              AS payment_late_cnt,
            COALESCE(pmh.months_since_last_late, 999)                   AS months_since_last_late,
            COALESCE(bureau.credit_score, 0)                           AS external_credit_score,
            COALESCE(vel.debit_7d, 0.00)                               AS debit_velocity_7d,
            COALESCE(vel.debit_30d, 0.00)                              AS debit_velocity_30d,
            COALESCE(new_merch.new_merch_30d, 0)                       AS new_merchant_cnt_30d,
            COALESCE(merch_risk.intl_txn_cnt, 0)                       AS international_txn_cnt,
            COALESCE(merch_risk.high_risk_cnt, 0)                      AS high_risk_merchant_cnt,
            now()                                                     AS load_ts
        FROM core_banking.customers c
        LEFT JOIN overdraft  ON c.customer_id = overdraft.customer_id
        LEFT JOIN lg_wd      ON c.customer_id = lg_wd.customer_id
        LEFT JOIN bal        ON c.customer_id = bal.customer_id
        LEFT JOIN credit     ON c.customer_id = credit.customer_id
        LEFT JOIN pmh        ON c.customer_id = pmh.customer_id
        LEFT JOIN bureau     ON c.customer_id = bureau.customer_id
        LEFT JOIN vel        ON c.customer_id = vel.customer_id
        LEFT JOIN new_merch  ON c.customer_id = new_merch.customer_id
        LEFT JOIN merch_risk ON c.customer_id = merch_risk.customer_id
        WHERE c.customer_status IN ('A', 'I')
    """)


# =============================================================================
# Phase 3 - SAS analytics (pandas + scikit-learn)
# =============================================================================
def phase3_python_analytics(con: duckdb.DuckDBPyConnection) -> None:
    """Reproduce the four SAS programs with pandas / scikit-learn."""
    con.execute("CREATE SCHEMA IF NOT EXISTS data_products")
    _sas_customer_segments(con)
    _sas_txn_analytics(con)
    _sas_risk_scoring(con)
    _sas_data_products(con)


def _write_table(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    con.register("_out_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _out_df")
    con.unregister("_out_df")


def _sas_customer_segments(con: duckdb.DuckDBPyConnection) -> None:
    """sas/01_sas_customer_segments.sas - PROC STDIZE + PROC FASTCLUS (k-means, k=5)."""
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    model_version = "SEG_V3.2"
    df = con.execute("""
        SELECT customer_id, age, tenure_months, customer_status, segment_code, state_code,
               num_accounts, num_active_accounts, has_checking, has_savings, has_credit,
               has_loan, total_balance, total_credit_limit, credit_utilization_pct
        FROM etl_staging.stg_customer_360
        WHERE customer_status = 'A'
    """).fetchdf()

    # Feature engineering
    df["product_breadth"] = (
        (df["has_checking"] == "Y").astype(int) + (df["has_savings"] == "Y").astype(int)
        + (df["has_credit"] == "Y").astype(int) + (df["has_loan"] == "Y").astype(int)
    ) / 4.0
    df["tenure_group"] = pd.cut(
        df["tenure_months"], bins=[-np.inf, 12, 36, 84, np.inf],
        right=False, labels=["NEW (<1yr)", "DEVELOPING (1-3yr)", "ESTABLISHED (3-7yr)", "LOYAL (7yr+)"]
    ).astype(str)
    df["age_group"] = pd.cut(
        df["age"], bins=[-np.inf, 25, 41, 57, 76, np.inf], right=False,
        labels=["GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER", "SILENT"]
    ).astype(str)
    df["balance_tier"] = pd.cut(
        df["total_balance"].astype(float), bins=[-np.inf, 1000, 10000, 100000, np.inf],
        right=False, labels=["LOW", "MODERATE", "AFFLUENT", "HIGH_NET_WORTH"]
    ).astype(str)
    df["digital_adoption_score"] = 0.0
    df["log_balance"] = np.log(np.maximum(df["total_balance"].astype(float), 1.0))
    df["acct_ratio"] = df["num_active_accounts"].astype(float) / np.maximum(df["num_accounts"].astype(float), 1.0)

    feature_cols = ["log_balance", "tenure_months", "credit_utilization_pct",
                    "product_breadth", "acct_ratio", "age"]
    feats = df[feature_cols].astype(float).fillna(0.0)
    standardised = StandardScaler().fit_transform(feats)

    k = min(5, len(df))
    km = KMeans(n_clusters=k, max_iter=50, n_init=10, random_state=SEED)
    df["cluster"] = km.fit_predict(standardised)

    # Order clusters by avg balance, label by rank (PROC SQL ... order by AVG_BALANCE desc)
    profile = df.groupby("cluster")["log_balance"].mean().sort_values(ascending=False)
    names = ["PREMIUM_WEALTH", "ENGAGED_MAINSTREAM", "GROWING_DIGITAL",
             "CREDIT_DEPENDENT", "VALUE_BASIC"]
    label_map = {cluster: names[i] for i, cluster in enumerate(profile.index)}
    df["segment_name"] = df["cluster"].map(label_map)
    # SAS PROC FASTCLUS numbers clusters 1..k; SEGMENT_ID = c.CLUSTER
    df["segment_id"] = df["cluster"] + 1

    df["subsegment_id"] = 0
    df["lifetime_value_score"] = (df["log_balance"] * df["tenure_months"]
                                  * df["product_breadth"] * 10).round(2)
    df["engagement_score"] = (df["acct_ratio"] * 100).round(2)
    df["product_breadth_index"] = (df["product_breadth"] * 100).round(2)
    df["channel_preference"] = ""
    df["cross_sell_flag"] = np.where((df["product_breadth"] < 0.50) & (df["acct_ratio"] >= 0.75), "Y", "N")
    df["upsell_flag"] = np.where((df["balance_tier"] == "MODERATE") & (df["tenure_group"] != "NEW (<1yr)"), "Y", "N")
    df["retention_risk_flag"] = np.where((df["acct_ratio"] < 0.50) & (df["tenure_months"] >= 60), "Y", "N")
    df["model_version"] = model_version
    df["effective_date"] = pd.Timestamp.now().normalize()
    df["load_ts"] = datetime.now()

    out = df[[
        "customer_id", "segment_name", "segment_id", "subsegment_id",
        "lifetime_value_score", "engagement_score", "digital_adoption_score",
        "product_breadth_index", "tenure_group", "age_group", "balance_tier",
        "channel_preference", "cross_sell_flag", "upsell_flag", "retention_risk_flag",
        "model_version", "effective_date", "load_ts"]]
    _write_table(con, "data_products.customer_segments", out)


def _sas_txn_analytics(con: duckdb.DuckDBPyConnection) -> None:
    """sas/02_sas_txn_analytics.sas - customer aggregation, PROC RANK, IQR anomaly."""
    model_version = "TXN_V2.1"
    reporting_period = pd.Timestamp.now().strftime("%Y-%m")
    stg = con.execute("SELECT * FROM etl_staging.stg_txn_summary").fetchdf()

    num_cols = ["txn_count_total", "amt_total_debit", "amt_total_credit", "amt_total_fees",
                "pct_web", "pct_mobile", "days_since_last_txn"]
    for c in num_cols:
        stg[c] = stg[c].astype(float)

    grp = stg.groupby("customer_id")
    df = pd.DataFrame({
        "total_accounts": grp["account_id"].nunique(),
        "active_accounts": grp.apply(lambda g: int((g["days_since_last_txn"] <= 30).sum()), include_groups=False),
        "total_transactions": grp["txn_count_total"].sum(),
        "total_debit_amt": grp["amt_total_debit"].sum(),
        "total_credit_amt": grp["amt_total_credit"].sum(),
        "total_fees": grp["amt_total_fees"].sum(),
        "top_spend_category": grp["top_merchant_category"].max(),
        "_digital_weighted": grp.apply(
            lambda g: (g["txn_count_total"] * (g["pct_web"] + g["pct_mobile"]) / 100).sum(),
            include_groups=False),
    }).reset_index()

    df["net_cash_flow"] = df["total_credit_amt"] - df["total_debit_amt"]
    df["avg_transaction_size"] = np.where(
        df["total_transactions"] > 0,
        (df["total_debit_amt"] + df["total_credit_amt"]) / df["total_transactions"].replace(0, np.nan), 0.0)
    df["digital_txn_pct"] = np.where(
        df["total_transactions"] > 0,
        df["_digital_weighted"] / df["total_transactions"].replace(0, np.nan) * 100, 0.0)

    df["monthly_spend_trend"] = np.select(
        [df["net_cash_flow"] > df["avg_transaction_size"] * 5,
         df["net_cash_flow"] < -df["avg_transaction_size"] * 5],
        ["UP", "DOWN"], default="STABLE")
    df["fee_income"] = df["total_fees"]
    df["interest_income"] = df["total_debit_amt"] * 0.02
    df["revenue_contribution"] = df["fee_income"] + df["interest_income"]

    # PROC RANK groups=100 -> integer percentile group 0..99
    # SAS formula: GROUP = FLOOR(RANK * k / (n + 1)), default ties=mean.
    _n = len(df)
    _rank = df["total_debit_amt"].rank(method="average")
    df["spend_percentile"] = np.floor(_rank * 100 / (_n + 1)).clip(0, 99).astype(float)

    # PROC MEANS IQR anomaly detection
    median = df["total_debit_amt"].median()
    iqr = df["total_debit_amt"].quantile(0.75) - df["total_debit_amt"].quantile(0.25)
    df["anomaly_flag"] = np.where(
        (df["total_debit_amt"] > median + 3 * iqr) & (iqr > 0), "Y", "N")

    df["reporting_period"] = reporting_period
    df["model_version"] = model_version
    df["effective_date"] = pd.Timestamp.now().normalize()
    df["load_ts"] = datetime.now()

    out = df[[
        "customer_id", "reporting_period", "total_accounts", "active_accounts",
        "total_transactions", "total_debit_amt", "total_credit_amt", "net_cash_flow",
        "avg_transaction_size", "monthly_spend_trend", "spend_percentile",
        "top_spend_category", "digital_txn_pct", "fee_income", "interest_income",
        "revenue_contribution", "anomaly_flag", "model_version", "effective_date", "load_ts"]]
    _write_table(con, "data_products.transaction_analytics", out)


def _sas_risk_scoring(con: duckdb.DuckDBPyConnection) -> None:
    """sas/03_sas_risk_scoring.sas - logistic-regression PD + weighted composite score."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    model_version = "RISK_V4.0"
    df = con.execute("""
        SELECT r.*, c.tenure_months, c.num_active_accounts, c.total_balance, c.customer_status
        FROM etl_staging.stg_risk_factors r
        JOIN etl_staging.stg_customer_360 c ON r.customer_id = c.customer_id
        WHERE c.customer_status = 'A'
    """).fetchdf()

    numeric = ["account_overdraft_cnt", "nsf_fee_total", "large_withdrawal_cnt",
               "large_withdrawal_amt", "avg_daily_balance_30d", "avg_daily_balance_90d",
               "balance_volatility", "credit_util_ratio", "payment_ontime_pct",
               "payment_late_cnt", "external_credit_score", "debit_velocity_7d",
               "debit_velocity_30d", "high_risk_merchant_cnt", "tenure_months"]
    for c in numeric:
        df[c] = df[c].astype(float)

    # Feature prep
    df.loc[(df["external_credit_score"] <= 0) | (df["external_credit_score"].isna()),
           "external_credit_score"] = 680
    df["bureau_score_norm"] = (df["external_credit_score"] - 300) / (850 - 300) * 100
    df["balance_trend_ratio"] = np.where(
        df["avg_daily_balance_90d"] > 0,
        df["avg_daily_balance_30d"] / df["avg_daily_balance_90d"].replace(0, np.nan), 1.0)
    df["velocity_ratio"] = np.where(
        df["debit_velocity_30d"] > 0,
        (df["debit_velocity_7d"] * (30 / 7)) / df["debit_velocity_30d"].replace(0, np.nan), 1.0)
    df["default_flag"] = (df["payment_late_cnt"] > 2).astype(int)

    model_features = ["bureau_score_norm", "credit_util_ratio", "payment_ontime_pct",
                      "balance_volatility", "velocity_ratio", "account_overdraft_cnt",
                      "large_withdrawal_cnt", "high_risk_merchant_cnt", "tenure_months"]
    X = df[model_features].astype(float).fillna(0.0)
    y = df["default_flag"]

    if y.nunique() < 2:
        df["prob_default"] = float(y.mean())
    else:
        model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
        model.fit(X, y)
        df["prob_default"] = model.predict_proba(X)[:, 1]

    # Composite scoring components (0-100)
    def clip(s):
        return np.clip(s, 0, 100)
    df["credit_risk_component"] = clip(100 - df["bureau_score_norm"])
    df["behaviour_risk_component"] = clip(100 - df["payment_ontime_pct"])
    df["velocity_risk_component"] = clip((df["velocity_ratio"] - 1) * 50)
    df["bureau_score_component"] = clip(df["bureau_score_norm"])
    df["payment_history_component"] = clip(df["payment_ontime_pct"])

    df["composite_risk_score"] = (
        df["credit_risk_component"] * 0.30 + df["behaviour_risk_component"] * 0.25
        + df["velocity_risk_component"] * 0.15 + (100 - df["bureau_score_component"]) * 0.20
        + (100 - df["payment_history_component"]) * 0.10).round(2)
    df["probability_of_default"] = df["prob_default"].fillna(0).round(6)

    df["risk_tier"] = pd.cut(
        df["composite_risk_score"], bins=[-np.inf, 20, 40, 60, 80, np.inf], right=False,
        labels=["LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"]).astype(str)

    df = _assign_risk_drivers(df)

    df["score_delta_30d"] = 0.0
    df["watch_list_flag"] = np.where(
        (df["risk_tier"] == "CRITICAL") & (df["probability_of_default"] > 0.5), "Y", "N")
    df["review_required_flag"] = np.where(
        (df["composite_risk_score"] >= 60) & (df["velocity_ratio"] > 2.0), "Y", "N")
    df["model_version"] = model_version
    df["effective_date"] = pd.Timestamp.now().normalize()
    df["load_ts"] = datetime.now()

    out = df[[
        "customer_id", "composite_risk_score", "risk_tier", "probability_of_default",
        "credit_risk_component", "behaviour_risk_component", "velocity_risk_component",
        "bureau_score_component", "payment_history_component", "primary_risk_driver",
        "secondary_risk_driver", "score_delta_30d", "watch_list_flag",
        "review_required_flag", "model_version", "effective_date", "load_ts"]]
    _write_table(con, "data_products.customer_risk_scores", out)


def _assign_risk_drivers(df: pd.DataFrame) -> pd.DataFrame:
    """Replicate the SAS top-two risk-driver selection loop (strict >, first wins ties)."""
    labels = ["CREDIT_UTILIZATION", "PAYMENT_BEHAVIOUR", "TRANSACTION_VELOCITY", "BUREAU_SCORE"]
    comp = np.column_stack([
        df["credit_risk_component"].to_numpy(),
        df["behaviour_risk_component"].to_numpy(),
        df["velocity_risk_component"].to_numpy(),
        (100 - df["bureau_score_component"]).to_numpy(),
    ])
    primary, secondary = [], []
    for row in comp:
        max1 = max2 = 0.0
        p = s = ""
        for i in range(4):
            if row[i] > max1:
                max2, s = max1, p
                max1, p = row[i], labels[i]
            elif row[i] > max2:
                max2, s = row[i], labels[i]
        primary.append(p)
        secondary.append(s)
    df["primary_risk_driver"] = primary
    df["secondary_risk_driver"] = secondary
    return df


def _sas_data_products(con: duckdb.DuckDBPyConnection) -> None:
    """sas/04_sas_data_products.sas - 4-way merge into the CUSTOMER_MASTER_PROFILE golden record."""
    model_version = "MASTER_V1.5"
    base = con.execute("""
        SELECT customer_id,
               trim(first_name) || ' ' || trim(last_name) AS full_name,
               age, state_code, customer_since, tenure_months, customer_status,
               num_accounts        AS total_accounts,
               num_active_accounts AS active_accounts,
               total_balance, total_credit_limit, credit_utilization_pct
        FROM etl_staging.stg_customer_360
        WHERE customer_status = 'A'
    """).fetchdf()

    segments = con.execute("""
        SELECT customer_id, segment_name, lifetime_value_score, engagement_score,
               cross_sell_flag, upsell_flag, retention_risk_flag
        FROM data_products.customer_segments
    """).fetchdf()
    txn = con.execute("""
        SELECT customer_id, total_transactions AS monthly_transactions,
               total_debit_amt AS monthly_spend, net_cash_flow, top_spend_category, digital_txn_pct
        FROM data_products.transaction_analytics
        WHERE effective_date = current_date
    """).fetchdf()
    risk = con.execute("""
        SELECT customer_id, composite_risk_score, risk_tier, probability_of_default, watch_list_flag
        FROM data_products.customer_risk_scores
    """).fetchdf()

    df = base.merge(segments, on="customer_id", how="left") \
             .merge(txn, on="customer_id", how="left") \
             .merge(risk, on="customer_id", how="left")

    # Defaults for customers missing in an upstream product
    seg_defaults = {"segment_name": "UNCLASSIFIED", "lifetime_value_score": 0,
                    "engagement_score": 0, "cross_sell_flag": "N", "upsell_flag": "N",
                    "retention_risk_flag": "N"}
    txn_defaults = {"monthly_transactions": 0, "monthly_spend": 0, "net_cash_flow": 0,
                    "top_spend_category": "", "digital_txn_pct": 0}
    risk_defaults = {"risk_tier": "UNKNOWN", "watch_list_flag": "N"}
    df = df.fillna({**seg_defaults, **txn_defaults, **risk_defaults})

    df["model_version"] = model_version
    df["effective_date"] = pd.Timestamp.now().normalize()
    df["load_ts"] = datetime.now()

    out = df[[
        "customer_id", "full_name", "age", "state_code", "customer_since", "tenure_months",
        "customer_status", "segment_name", "lifetime_value_score", "engagement_score",
        "total_accounts", "active_accounts", "total_balance", "total_credit_limit",
        "credit_utilization_pct", "monthly_transactions", "monthly_spend", "net_cash_flow",
        "top_spend_category", "digital_txn_pct", "composite_risk_score", "risk_tier",
        "probability_of_default", "watch_list_flag", "cross_sell_flag", "upsell_flag",
        "retention_risk_flag", "model_version", "effective_date", "load_ts"]]
    _write_table(con, "data_products.customer_master_profile", out)


# =============================================================================
# Standalone convenience runner (export_data.py is the primary entrypoint)
# =============================================================================
def build_all(con: duckdb.DuckDBPyConnection, n_customers: int = 5000) -> None:
    """Create schemas + source tables, then run all three phases on `con`."""
    for schema in ("core_banking", "txn_processing", "etl_staging", "data_products"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute("CREATE OR REPLACE TABLE core_banking.customers (customer_id BIGINT PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, date_of_birth DATE, ssn_hash VARCHAR, email VARCHAR, phone_primary VARCHAR, customer_since DATE, customer_status VARCHAR, segment_code VARCHAR, branch_id INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE core_banking.accounts (account_id BIGINT PRIMARY KEY, customer_id BIGINT, account_type VARCHAR, account_status VARCHAR, open_date DATE, close_date DATE, current_balance DECIMAL(15,2), available_balance DECIMAL(15,2), credit_limit DECIMAL(15,2), interest_rate DECIMAL(5,4), branch_id INTEGER, created_ts TIMESTAMP, updated_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE core_banking.addresses (address_id BIGINT PRIMARY KEY, customer_id BIGINT, address_type VARCHAR, address_line_1 VARCHAR, address_line_2 VARCHAR, city VARCHAR, state_code VARCHAR, zip_code VARCHAR, country_code VARCHAR, is_primary VARCHAR, effective_date DATE, expiration_date DATE, created_ts TIMESTAMP, updated_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE txn_processing.transactions (transaction_id BIGINT PRIMARY KEY, account_id BIGINT, transaction_type_cd VARCHAR, transaction_date DATE, transaction_ts TIMESTAMP, amount DECIMAL(15,2), running_balance DECIMAL(15,2), merchant_name VARCHAR, merchant_category VARCHAR, channel_code VARCHAR, reference_num VARCHAR, status_code VARCHAR, created_ts TIMESTAMP)")
    con.execute("CREATE OR REPLACE TABLE txn_processing.transaction_types (transaction_type_cd VARCHAR PRIMARY KEY, description VARCHAR, category VARCHAR, is_revenue VARCHAR, effective_date DATE, expiration_date DATE)")
    con.execute("CREATE OR REPLACE TABLE core_banking.customer_bureau_scores (customer_id BIGINT, external_credit_score INTEGER, report_date DATE)")
    _builtin_populate_sources(con, n_customers=n_customers)
    phase2_bteq_transforms(con)
    phase3_python_analytics(con)


if __name__ == "__main__":
    connection = duckdb.connect(":memory:")
    build_all(connection, n_customers=500)
    for tbl in ("etl_staging.stg_customer_360", "etl_staging.stg_txn_summary",
                "etl_staging.stg_risk_factors", "data_products.customer_segments",
                "data_products.transaction_analytics", "data_products.customer_risk_scores",
                "data_products.customer_master_profile"):
        n = connection.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"{tbl:45s} {n:>8,} rows")
    connection.close()
