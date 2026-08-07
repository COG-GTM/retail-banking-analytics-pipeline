"""Explicit schemas for every layer, transcribed column-for-column from ``ddl/``.

Teradata type mapping used throughout:

===========================  ==========================
Teradata                     Delta / Spark
===========================  ==========================
``BIGINT``                   ``bigint``
``INTEGER``                  ``int``
``SMALLINT``                 ``smallint``
``DECIMAL(p,s)``             ``decimal(p,s)``
``CHAR(n)`` / ``VARCHAR(n)`` ``string``
``DATE``                     ``date``
``TIMESTAMP(6)``             ``timestamp``
===========================  ==========================

``PRIMARY INDEX`` and ``COLLECT STATISTICS`` have no Delta equivalent and are
dropped; clustering is expressed with ``CLUSTER BY`` / ``ZORDER`` instead.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Bronze — ddl/00_source_tables.sql
# ---------------------------------------------------------------------------
SOURCE_SCHEMAS: dict[str, str] = {
    "CUSTOMERS": """
        CUSTOMER_ID     bigint,
        FIRST_NAME      string,
        LAST_NAME       string,
        DATE_OF_BIRTH   date,
        SSN_HASH        string,
        EMAIL           string,
        PHONE_PRIMARY   string,
        CUSTOMER_SINCE  date,
        CUSTOMER_STATUS string,
        SEGMENT_CODE    string,
        BRANCH_ID       int,
        CREATED_TS      timestamp,
        UPDATED_TS      timestamp
    """,
    "ACCOUNTS": """
        ACCOUNT_ID        bigint,
        CUSTOMER_ID       bigint,
        ACCOUNT_TYPE      string,
        ACCOUNT_STATUS    string,
        OPEN_DATE         date,
        CLOSE_DATE        date,
        CURRENT_BALANCE   decimal(15,2),
        AVAILABLE_BALANCE decimal(15,2),
        CREDIT_LIMIT      decimal(15,2),
        INTEREST_RATE     decimal(5,4),
        BRANCH_ID         int,
        CREATED_TS        timestamp,
        UPDATED_TS        timestamp
    """,
    "ADDRESSES": """
        ADDRESS_ID      bigint,
        CUSTOMER_ID     bigint,
        ADDRESS_TYPE    string,
        ADDRESS_LINE_1  string,
        ADDRESS_LINE_2  string,
        CITY            string,
        STATE_CODE      string,
        ZIP_CODE        string,
        COUNTRY_CODE    string,
        IS_PRIMARY      string,
        EFFECTIVE_DATE  date,
        EXPIRATION_DATE date,
        CREATED_TS      timestamp,
        UPDATED_TS      timestamp
    """,
    "TRANSACTIONS": """
        TRANSACTION_ID      bigint,
        ACCOUNT_ID          bigint,
        TRANSACTION_TYPE_CD string,
        TRANSACTION_DATE    date,
        TRANSACTION_TS      timestamp,
        AMOUNT              decimal(15,2),
        RUNNING_BALANCE     decimal(15,2),
        MERCHANT_NAME       string,
        MERCHANT_CATEGORY   string,
        CHANNEL_CODE        string,
        REFERENCE_NUM       string,
        STATUS_CODE         string,
        CREATED_TS          timestamp
    """,
    "TRANSACTION_TYPES": """
        TRANSACTION_TYPE_CD string,
        DESCRIPTION         string,
        CATEGORY            string,
        IS_REVENUE          string,
        EFFECTIVE_DATE      date,
        EXPIRATION_DATE     date
    """,
    # Referenced by 03_stg_risk_factors.bteq; absent from ddl/00 but present in
    # data/01_source_tables/customer_bureau_scores.csv.
    "CUSTOMER_BUREAU_SCORES": """
        CUSTOMER_ID           bigint,
        EXTERNAL_CREDIT_SCORE int,
        REPORT_DATE           date
    """,
}

# CSV file name (data/01_source_tables) -> bronze Delta table
SOURCE_FILES: dict[str, str] = {
    "customers": "CUSTOMERS",
    "accounts": "ACCOUNTS",
    "addresses": "ADDRESSES",
    "transactions": "TRANSACTIONS",
    "transaction_types": "TRANSACTION_TYPES",
    "customer_bureau_scores": "CUSTOMER_BUREAU_SCORES",
}

# ---------------------------------------------------------------------------
# Silver — ddl/01_staging_tables.sql
# ---------------------------------------------------------------------------
STG_CUSTOMER_360: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("FIRST_NAME", "string"),
    ("LAST_NAME", "string"),
    ("DATE_OF_BIRTH", "date"),
    ("AGE", "smallint"),
    ("CUSTOMER_SINCE", "date"),
    ("TENURE_MONTHS", "int"),
    ("CUSTOMER_STATUS", "string"),
    ("SEGMENT_CODE", "string"),
    ("BRANCH_ID", "int"),
    ("PRIMARY_ADDRESS", "string"),
    ("CITY", "string"),
    ("STATE_CODE", "string"),
    ("ZIP_CODE", "string"),
    ("NUM_ACCOUNTS", "smallint"),
    ("NUM_ACTIVE_ACCOUNTS", "smallint"),
    ("HAS_CHECKING", "string"),
    ("HAS_SAVINGS", "string"),
    ("HAS_CREDIT", "string"),
    ("HAS_LOAN", "string"),
    ("TOTAL_BALANCE", "decimal(18,2)"),
    ("TOTAL_CREDIT_LIMIT", "decimal(18,2)"),
    ("CREDIT_UTILIZATION_PCT", "decimal(5,2)"),
    ("LOAD_TS", "timestamp"),
]

STG_TXN_SUMMARY: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("ACCOUNT_ID", "bigint"),
    ("ACCOUNT_TYPE", "string"),
    ("SUMMARY_PERIOD_START", "date"),
    ("SUMMARY_PERIOD_END", "date"),
    ("TXN_COUNT_TOTAL", "int"),
    ("TXN_COUNT_DEBIT", "int"),
    ("TXN_COUNT_CREDIT", "int"),
    ("TXN_COUNT_FEE", "int"),
    ("AMT_TOTAL_DEBIT", "decimal(18,2)"),
    ("AMT_TOTAL_CREDIT", "decimal(18,2)"),
    ("AMT_TOTAL_FEES", "decimal(18,2)"),
    ("AMT_AVG_DEBIT", "decimal(15,2)"),
    ("AMT_AVG_CREDIT", "decimal(15,2)"),
    ("AMT_MAX_SINGLE_DEBIT", "decimal(15,2)"),
    ("AMT_MAX_SINGLE_CREDIT", "decimal(15,2)"),
    ("DISTINCT_MERCHANTS", "int"),
    ("TOP_MERCHANT_CATEGORY", "string"),
    ("PCT_ATM", "decimal(5,2)"),
    ("PCT_POS", "decimal(5,2)"),
    ("PCT_WEB", "decimal(5,2)"),
    ("PCT_MOBILE", "decimal(5,2)"),
    ("DAYS_SINCE_LAST_TXN", "int"),
    ("LOAD_TS", "timestamp"),
]

STG_RISK_FACTORS: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("ACCOUNT_OVERDRAFT_CNT", "int"),
    ("NSF_FEE_TOTAL", "decimal(15,2)"),
    ("LARGE_WITHDRAWAL_CNT", "int"),
    ("LARGE_WITHDRAWAL_AMT", "decimal(18,2)"),
    ("AVG_DAILY_BALANCE_30D", "decimal(15,2)"),
    ("AVG_DAILY_BALANCE_90D", "decimal(15,2)"),
    ("BALANCE_VOLATILITY", "decimal(10,4)"),
    ("CREDIT_UTIL_RATIO", "decimal(5,4)"),
    ("PAYMENT_ONTIME_PCT", "decimal(5,2)"),
    ("PAYMENT_LATE_CNT", "int"),
    ("MONTHS_SINCE_LAST_LATE", "int"),
    ("EXTERNAL_CREDIT_SCORE", "int"),
    ("DEBIT_VELOCITY_7D", "decimal(15,2)"),
    ("DEBIT_VELOCITY_30D", "decimal(15,2)"),
    ("NEW_MERCHANT_CNT_30D", "int"),
    ("INTERNATIONAL_TXN_CNT", "int"),
    ("HIGH_RISK_MERCHANT_CNT", "int"),
    ("LOAD_TS", "timestamp"),
]

# ---------------------------------------------------------------------------
# Gold — ddl/02_data_product_tables.sql (the downstream contract)
# ---------------------------------------------------------------------------
CUSTOMER_SEGMENTS: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("SEGMENT_NAME", "string"),
    ("SEGMENT_ID", "smallint"),
    ("SUBSEGMENT_ID", "smallint"),
    ("LIFETIME_VALUE_SCORE", "decimal(10,2)"),
    ("ENGAGEMENT_SCORE", "decimal(5,2)"),
    ("DIGITAL_ADOPTION_SCORE", "decimal(5,2)"),
    ("PRODUCT_BREADTH_INDEX", "decimal(5,2)"),
    ("TENURE_GROUP", "string"),
    ("AGE_GROUP", "string"),
    ("BALANCE_TIER", "string"),
    ("CHANNEL_PREFERENCE", "string"),
    ("CROSS_SELL_FLAG", "string"),
    ("UPSELL_FLAG", "string"),
    ("RETENTION_RISK_FLAG", "string"),
    ("MODEL_VERSION", "string"),
    ("EFFECTIVE_DATE", "date"),
    ("LOAD_TS", "timestamp"),
]

TRANSACTION_ANALYTICS: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("REPORTING_PERIOD", "string"),
    ("TOTAL_ACCOUNTS", "smallint"),
    ("ACTIVE_ACCOUNTS", "smallint"),
    ("TOTAL_TRANSACTIONS", "int"),
    ("TOTAL_DEBIT_AMT", "decimal(18,2)"),
    ("TOTAL_CREDIT_AMT", "decimal(18,2)"),
    ("NET_CASH_FLOW", "decimal(18,2)"),
    ("AVG_TRANSACTION_SIZE", "decimal(15,2)"),
    ("MONTHLY_SPEND_TREND", "string"),
    ("SPEND_PERCENTILE", "decimal(5,2)"),
    ("TOP_SPEND_CATEGORY", "string"),
    ("DIGITAL_TXN_PCT", "decimal(5,2)"),
    ("FEE_INCOME", "decimal(15,2)"),
    ("INTEREST_INCOME", "decimal(15,2)"),
    ("REVENUE_CONTRIBUTION", "decimal(15,2)"),
    ("ANOMALY_FLAG", "string"),
    ("MODEL_VERSION", "string"),
    ("EFFECTIVE_DATE", "date"),
    ("LOAD_TS", "timestamp"),
]

CUSTOMER_RISK_SCORES: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("COMPOSITE_RISK_SCORE", "decimal(6,2)"),
    ("RISK_TIER", "string"),
    ("PROBABILITY_OF_DEFAULT", "decimal(7,6)"),
    ("CREDIT_RISK_COMPONENT", "decimal(5,2)"),
    ("BEHAVIOUR_RISK_COMPONENT", "decimal(5,2)"),
    ("VELOCITY_RISK_COMPONENT", "decimal(5,2)"),
    ("BUREAU_SCORE_COMPONENT", "decimal(5,2)"),
    ("PAYMENT_HISTORY_COMPONENT", "decimal(5,2)"),
    ("PRIMARY_RISK_DRIVER", "string"),
    ("SECONDARY_RISK_DRIVER", "string"),
    ("SCORE_DELTA_30D", "decimal(6,2)"),
    ("WATCH_LIST_FLAG", "string"),
    ("REVIEW_REQUIRED_FLAG", "string"),
    ("MODEL_VERSION", "string"),
    ("EFFECTIVE_DATE", "date"),
    ("LOAD_TS", "timestamp"),
]

CUSTOMER_MASTER_PROFILE: list[tuple[str, str]] = [
    ("CUSTOMER_ID", "bigint"),
    ("FULL_NAME", "string"),
    ("AGE", "smallint"),
    ("STATE_CODE", "string"),
    ("CUSTOMER_SINCE", "date"),
    ("TENURE_MONTHS", "int"),
    ("CUSTOMER_STATUS", "string"),
    ("SEGMENT_NAME", "string"),
    ("LIFETIME_VALUE_SCORE", "decimal(10,2)"),
    ("ENGAGEMENT_SCORE", "decimal(5,2)"),
    ("TOTAL_ACCOUNTS", "smallint"),
    ("ACTIVE_ACCOUNTS", "smallint"),
    ("TOTAL_BALANCE", "decimal(18,2)"),
    ("TOTAL_CREDIT_LIMIT", "decimal(18,2)"),
    ("CREDIT_UTILIZATION_PCT", "decimal(5,2)"),
    ("MONTHLY_TRANSACTIONS", "int"),
    ("MONTHLY_SPEND", "decimal(18,2)"),
    ("NET_CASH_FLOW", "decimal(18,2)"),
    ("TOP_SPEND_CATEGORY", "string"),
    ("DIGITAL_TXN_PCT", "decimal(5,2)"),
    ("COMPOSITE_RISK_SCORE", "decimal(6,2)"),
    ("RISK_TIER", "string"),
    ("PROBABILITY_OF_DEFAULT", "decimal(7,6)"),
    ("WATCH_LIST_FLAG", "string"),
    ("CROSS_SELL_FLAG", "string"),
    ("UPSELL_FLAG", "string"),
    ("RETENTION_RISK_FLAG", "string"),
    ("MODEL_VERSION", "string"),
    ("EFFECTIVE_DATE", "date"),
    ("LOAD_TS", "timestamp"),
]

GOLD_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "CUSTOMER_SEGMENTS": CUSTOMER_SEGMENTS,
    "TRANSACTION_ANALYTICS": TRANSACTION_ANALYTICS,
    "CUSTOMER_RISK_SCORES": CUSTOMER_RISK_SCORES,
    "CUSTOMER_MASTER_PROFILE": CUSTOMER_MASTER_PROFILE,
}


def source_schema(table: str) -> StructType:
    """Return the bronze schema of ``table`` as a :class:`StructType`."""
    from pyspark.sql.types import _parse_datatype_string

    return _parse_datatype_string(SOURCE_SCHEMAS[table])


def conform(df: DataFrame, spec: list[tuple[str, str]]) -> DataFrame:
    """Project ``df`` onto ``spec``: exact column order, names and types."""
    return df.select(*[F.col(name).cast(dtype).alias(name) for name, dtype in spec])
