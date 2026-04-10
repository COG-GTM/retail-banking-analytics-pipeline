# /// script
# requires-python = ">=3.10"
# dependencies = ["faker>=28.0", "teradatasql>=20.0", "pandas>=2.0"]
# ///
"""Load synthetic banking data into a Teradata ClearScape instance."""

import argparse
import sys
from pathlib import Path

# Import the shared data generator
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import generate_data

import teradatasql
import pandas as pd


def create_source_tables(con, cursor):
    """Create source tables using DDL adapted for ClearScape (single-database)."""
    # In ClearScape the user's default database is used for everything.
    # We adapt the schema: instead of CORE_BANKING_DB.CUSTOMERS we just use CUSTOMERS.
    # The BTEQ scripts will be run with a wrapper that aliases database names.

    ddl_statements = [
        # -- Source Tables --
        """CREATE MULTISET TABLE CUSTOMERS, NO FALLBACK (
            CUSTOMER_ID BIGINT NOT NULL, FIRST_NAME VARCHAR(60), LAST_NAME VARCHAR(60),
            DATE_OF_BIRTH DATE FORMAT 'YYYY-MM-DD', SSN_HASH CHAR(64),
            EMAIL VARCHAR(120), PHONE_PRIMARY VARCHAR(20),
            CUSTOMER_SINCE DATE FORMAT 'YYYY-MM-DD', CUSTOMER_STATUS CHAR(1),
            SEGMENT_CODE VARCHAR(10), BRANCH_ID INTEGER,
            CREATED_TS TIMESTAMP(6), UPDATED_TS TIMESTAMP(6)
        ) PRIMARY INDEX (CUSTOMER_ID)""",

        """CREATE MULTISET TABLE ACCOUNTS, NO FALLBACK (
            ACCOUNT_ID BIGINT NOT NULL, CUSTOMER_ID BIGINT NOT NULL,
            ACCOUNT_TYPE VARCHAR(20), ACCOUNT_STATUS CHAR(1),
            OPEN_DATE DATE FORMAT 'YYYY-MM-DD', CLOSE_DATE DATE FORMAT 'YYYY-MM-DD',
            CURRENT_BALANCE DECIMAL(15,2), AVAILABLE_BALANCE DECIMAL(15,2),
            CREDIT_LIMIT DECIMAL(15,2), INTEREST_RATE DECIMAL(5,4),
            BRANCH_ID INTEGER, CREATED_TS TIMESTAMP(6), UPDATED_TS TIMESTAMP(6)
        ) PRIMARY INDEX (ACCOUNT_ID)""",

        """CREATE MULTISET TABLE ADDRESSES, NO FALLBACK (
            ADDRESS_ID BIGINT NOT NULL, CUSTOMER_ID BIGINT NOT NULL,
            ADDRESS_TYPE VARCHAR(10), ADDRESS_LINE_1 VARCHAR(100), ADDRESS_LINE_2 VARCHAR(100),
            CITY VARCHAR(60), STATE_CODE CHAR(2), ZIP_CODE VARCHAR(10),
            COUNTRY_CODE CHAR(2) DEFAULT 'US', IS_PRIMARY CHAR(1) DEFAULT 'N',
            EFFECTIVE_DATE DATE FORMAT 'YYYY-MM-DD', EXPIRATION_DATE DATE FORMAT 'YYYY-MM-DD',
            CREATED_TS TIMESTAMP(6), UPDATED_TS TIMESTAMP(6)
        ) PRIMARY INDEX (ADDRESS_ID)""",

        """CREATE MULTISET TABLE TRANSACTION_TYPES, NO FALLBACK (
            TRANSACTION_TYPE_CD VARCHAR(10) NOT NULL, DESCRIPTION VARCHAR(60),
            CATEGORY VARCHAR(30), IS_REVENUE CHAR(1) DEFAULT 'N',
            EFFECTIVE_DATE DATE FORMAT 'YYYY-MM-DD', EXPIRATION_DATE DATE FORMAT 'YYYY-MM-DD'
        ) UNIQUE PRIMARY INDEX (TRANSACTION_TYPE_CD)""",

        """CREATE MULTISET TABLE TRANSACTIONS, NO FALLBACK (
            TRANSACTION_ID BIGINT NOT NULL, ACCOUNT_ID BIGINT NOT NULL,
            TRANSACTION_TYPE_CD VARCHAR(10),
            TRANSACTION_DATE DATE FORMAT 'YYYY-MM-DD', TRANSACTION_TS TIMESTAMP(6),
            AMOUNT DECIMAL(15,2), RUNNING_BALANCE DECIMAL(15,2),
            MERCHANT_NAME VARCHAR(100), MERCHANT_CATEGORY VARCHAR(60),
            CHANNEL_CODE VARCHAR(10), REFERENCE_NUM VARCHAR(40),
            STATUS_CODE CHAR(1), CREATED_TS TIMESTAMP(6)
        ) PRIMARY INDEX (TRANSACTION_ID)""",

        """CREATE MULTISET TABLE CUSTOMER_BUREAU_SCORES, NO FALLBACK (
            CUSTOMER_ID BIGINT NOT NULL,
            EXTERNAL_CREDIT_SCORE INTEGER,
            REPORT_DATE DATE FORMAT 'YYYY-MM-DD'
        ) PRIMARY INDEX (CUSTOMER_ID)""",

        """CREATE MULTISET TABLE ETL_RUN_LOG, NO FALLBACK (
            JOB_NAME VARCHAR(60), STEP_NAME VARCHAR(60), STATUS VARCHAR(20),
            ROW_COUNT INTEGER, START_TS TIMESTAMP(6), END_TS TIMESTAMP(6)
        ) PRIMARY INDEX (JOB_NAME)""",
    ]

    for ddl in ddl_statements:
        table_name = ddl.split("TABLE")[1].split(",")[0].strip()
        try:
            cursor.execute(f"DROP TABLE {table_name}")
        except Exception:
            pass
        print(f"  Creating {table_name}...", file=sys.stderr)
        cursor.execute(ddl)


def load_dataframe(cursor, table_name, df):
    """Bulk insert a pandas DataFrame into a Teradata table."""
    if len(df) == 0:
        return
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    # Convert to list of tuples, handling NaT/NaN -> None
    rows = []
    for _, row in df.iterrows():
        vals = []
        for v in row:
            if pd.isna(v):
                vals.append(None)
            else:
                vals.append(v)
        rows.append(tuple(vals))

    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)

    print(f"  Loaded {len(rows)} rows into {table_name}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Load synthetic data into Teradata ClearScape")
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--customers", type=int, default=5000)
    args = parser.parse_args()

    print(f"Generating synthetic data ({args.customers} customers)...", file=sys.stderr)
    customers = generate_data.generate_customers(args.customers)
    accounts = generate_data.generate_accounts(customers)
    addresses = generate_data.generate_addresses(customers)
    txn_types = generate_data.generate_transaction_types()
    transactions = generate_data.generate_transactions(accounts, txn_types)
    bureau = generate_data.generate_bureau_scores(customers)

    print(f"Connecting to {args.host}...", file=sys.stderr)
    with teradatasql.connect(host=args.host, user=args.user, password=args.password) as con:
        cursor = con.cursor()

        print("Creating tables...", file=sys.stderr)
        create_source_tables(con, cursor)

        print("Loading data...", file=sys.stderr)
        load_dataframe(cursor, "CUSTOMERS", customers)
        load_dataframe(cursor, "ACCOUNTS", accounts)
        load_dataframe(cursor, "ADDRESSES", addresses)
        load_dataframe(cursor, "TRANSACTION_TYPES", txn_types)
        load_dataframe(cursor, "TRANSACTIONS", transactions)
        load_dataframe(cursor, "CUSTOMER_BUREAU_SCORES", bureau)

    print("\nData load complete!", file=sys.stderr)
    print(f"  Customers:    {len(customers)}")
    print(f"  Accounts:     {len(accounts)}")
    print(f"  Addresses:    {len(addresses)}")
    print(f"  Transactions: {len(transactions)}")
    print(f"  Bureau scores:{len(bureau)}")


if __name__ == "__main__":
    main()
