"""Table contracts, transcribed column-for-column from the Teradata DDL.

``ddl/00_source_tables.sql``      -> :data:`SOURCE_SCHEMAS`
``ddl/01_staging_tables.sql``     -> :data:`SILVER_SCHEMAS`
``ddl/02_data_product_tables.sql``-> :data:`GOLD_SCHEMAS`

Teradata type mapping used throughout:

======================  ==========================
Teradata                Spark / Delta
======================  ==========================
BIGINT                  LongType
INTEGER                 IntegerType
SMALLINT                ShortType
DECIMAL(p,s)            DecimalType(p, s)
CHAR(n) / VARCHAR(n)    StringType
DATE                    DateType
TIMESTAMP(6)            TimestampType
======================  ==========================

:func:`conform` is applied immediately before every write so a target table can
never drift from its contract (column set, order, and type).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _f(name: str, dtype, nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable)


# ---------------------------------------------------------------------------
# Bronze — ddl/00_source_tables.sql
# ---------------------------------------------------------------------------
SOURCE_SCHEMAS: dict[str, StructType] = {
    "CUSTOMERS": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("FIRST_NAME", StringType()),
        _f("LAST_NAME", StringType()),
        _f("DATE_OF_BIRTH", DateType()),
        _f("SSN_HASH", StringType()),
        _f("EMAIL", StringType()),
        _f("PHONE_PRIMARY", StringType()),
        _f("CUSTOMER_SINCE", DateType()),
        _f("CUSTOMER_STATUS", StringType()),
        _f("SEGMENT_CODE", StringType()),
        _f("BRANCH_ID", IntegerType()),
        _f("CREATED_TS", TimestampType()),
        _f("UPDATED_TS", TimestampType()),
    ]),
    "ACCOUNTS": StructType([
        _f("ACCOUNT_ID", LongType(), False),
        _f("CUSTOMER_ID", LongType(), False),
        _f("ACCOUNT_TYPE", StringType()),
        _f("ACCOUNT_STATUS", StringType()),
        _f("OPEN_DATE", DateType()),
        _f("CLOSE_DATE", DateType()),
        _f("CURRENT_BALANCE", DecimalType(15, 2)),
        _f("AVAILABLE_BALANCE", DecimalType(15, 2)),
        _f("CREDIT_LIMIT", DecimalType(15, 2)),
        _f("INTEREST_RATE", DecimalType(5, 4)),
        _f("BRANCH_ID", IntegerType()),
        _f("CREATED_TS", TimestampType()),
        _f("UPDATED_TS", TimestampType()),
    ]),
    "ADDRESSES": StructType([
        _f("ADDRESS_ID", LongType(), False),
        _f("CUSTOMER_ID", LongType(), False),
        _f("ADDRESS_TYPE", StringType()),
        _f("ADDRESS_LINE_1", StringType()),
        _f("ADDRESS_LINE_2", StringType()),
        _f("CITY", StringType()),
        _f("STATE_CODE", StringType()),
        _f("ZIP_CODE", StringType()),
        _f("COUNTRY_CODE", StringType()),
        _f("IS_PRIMARY", StringType()),
        _f("EFFECTIVE_DATE", DateType()),
        _f("EXPIRATION_DATE", DateType()),
        _f("CREATED_TS", TimestampType()),
        _f("UPDATED_TS", TimestampType()),
    ]),
    "TRANSACTIONS": StructType([
        _f("TRANSACTION_ID", LongType(), False),
        _f("ACCOUNT_ID", LongType(), False),
        _f("TRANSACTION_TYPE_CD", StringType()),
        _f("TRANSACTION_DATE", DateType()),
        _f("TRANSACTION_TS", TimestampType()),
        _f("AMOUNT", DecimalType(15, 2)),
        _f("RUNNING_BALANCE", DecimalType(15, 2)),
        _f("MERCHANT_NAME", StringType()),
        _f("MERCHANT_CATEGORY", StringType()),
        _f("CHANNEL_CODE", StringType()),
        _f("REFERENCE_NUM", StringType()),
        _f("STATUS_CODE", StringType()),
        _f("CREATED_TS", TimestampType()),
    ]),
    "TRANSACTION_TYPES": StructType([
        _f("TRANSACTION_TYPE_CD", StringType(), False),
        _f("DESCRIPTION", StringType()),
        _f("CATEGORY", StringType()),
        _f("IS_REVENUE", StringType()),
        _f("EFFECTIVE_DATE", DateType()),
        _f("EXPIRATION_DATE", DateType()),
    ]),
    # Referenced by bteq/03_stg_risk_factors.bteq; no DDL shipped with the demo.
    "CUSTOMER_BUREAU_SCORES": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("EXTERNAL_CREDIT_SCORE", IntegerType()),
        _f("REPORT_DATE", DateType()),
    ]),
}

# CSV file (as written by export_data.py) -> bronze table name
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
SILVER_SCHEMAS: dict[str, StructType] = {
    "STG_CUSTOMER_360": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("FIRST_NAME", StringType()),
        _f("LAST_NAME", StringType()),
        _f("DATE_OF_BIRTH", DateType()),
        _f("AGE", ShortType()),
        _f("CUSTOMER_SINCE", DateType()),
        _f("TENURE_MONTHS", IntegerType()),
        _f("CUSTOMER_STATUS", StringType()),
        _f("SEGMENT_CODE", StringType()),
        _f("BRANCH_ID", IntegerType()),
        _f("PRIMARY_ADDRESS", StringType()),
        _f("CITY", StringType()),
        _f("STATE_CODE", StringType()),
        _f("ZIP_CODE", StringType()),
        _f("NUM_ACCOUNTS", ShortType()),
        _f("NUM_ACTIVE_ACCOUNTS", ShortType()),
        _f("HAS_CHECKING", StringType()),
        _f("HAS_SAVINGS", StringType()),
        _f("HAS_CREDIT", StringType()),
        _f("HAS_LOAN", StringType()),
        _f("TOTAL_BALANCE", DecimalType(18, 2)),
        _f("TOTAL_CREDIT_LIMIT", DecimalType(18, 2)),
        _f("CREDIT_UTILIZATION_PCT", DecimalType(5, 2)),
        _f("LOAD_TS", TimestampType()),
    ]),
    "STG_TXN_SUMMARY": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("ACCOUNT_ID", LongType(), False),
        _f("ACCOUNT_TYPE", StringType()),
        _f("SUMMARY_PERIOD_START", DateType()),
        _f("SUMMARY_PERIOD_END", DateType()),
        _f("TXN_COUNT_TOTAL", IntegerType()),
        _f("TXN_COUNT_DEBIT", IntegerType()),
        _f("TXN_COUNT_CREDIT", IntegerType()),
        _f("TXN_COUNT_FEE", IntegerType()),
        _f("AMT_TOTAL_DEBIT", DecimalType(18, 2)),
        _f("AMT_TOTAL_CREDIT", DecimalType(18, 2)),
        _f("AMT_TOTAL_FEES", DecimalType(18, 2)),
        _f("AMT_AVG_DEBIT", DecimalType(15, 2)),
        _f("AMT_AVG_CREDIT", DecimalType(15, 2)),
        _f("AMT_MAX_SINGLE_DEBIT", DecimalType(15, 2)),
        _f("AMT_MAX_SINGLE_CREDIT", DecimalType(15, 2)),
        _f("DISTINCT_MERCHANTS", IntegerType()),
        _f("TOP_MERCHANT_CATEGORY", StringType()),
        _f("PCT_ATM", DecimalType(5, 2)),
        _f("PCT_POS", DecimalType(5, 2)),
        _f("PCT_WEB", DecimalType(5, 2)),
        _f("PCT_MOBILE", DecimalType(5, 2)),
        _f("DAYS_SINCE_LAST_TXN", IntegerType()),
        _f("LOAD_TS", TimestampType()),
    ]),
    "STG_RISK_FACTORS": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("ACCOUNT_OVERDRAFT_CNT", IntegerType()),
        _f("NSF_FEE_TOTAL", DecimalType(15, 2)),
        _f("LARGE_WITHDRAWAL_CNT", IntegerType()),
        _f("LARGE_WITHDRAWAL_AMT", DecimalType(18, 2)),
        _f("AVG_DAILY_BALANCE_30D", DecimalType(15, 2)),
        _f("AVG_DAILY_BALANCE_90D", DecimalType(15, 2)),
        _f("BALANCE_VOLATILITY", DecimalType(10, 4)),
        _f("CREDIT_UTIL_RATIO", DecimalType(5, 4)),
        _f("PAYMENT_ONTIME_PCT", DecimalType(5, 2)),
        _f("PAYMENT_LATE_CNT", IntegerType()),
        _f("MONTHS_SINCE_LAST_LATE", IntegerType()),
        _f("EXTERNAL_CREDIT_SCORE", IntegerType()),
        _f("DEBIT_VELOCITY_7D", DecimalType(15, 2)),
        _f("DEBIT_VELOCITY_30D", DecimalType(15, 2)),
        _f("NEW_MERCHANT_CNT_30D", IntegerType()),
        _f("INTERNATIONAL_TXN_CNT", IntegerType()),
        _f("HIGH_RISK_MERCHANT_CNT", IntegerType()),
        _f("LOAD_TS", TimestampType()),
    ]),
}

# ---------------------------------------------------------------------------
# Gold — ddl/02_data_product_tables.sql
# ---------------------------------------------------------------------------
GOLD_SCHEMAS: dict[str, StructType] = {
    "CUSTOMER_SEGMENTS": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("SEGMENT_NAME", StringType()),
        _f("SEGMENT_ID", ShortType()),
        _f("SUBSEGMENT_ID", ShortType()),
        _f("LIFETIME_VALUE_SCORE", DecimalType(10, 2)),
        _f("ENGAGEMENT_SCORE", DecimalType(5, 2)),
        _f("DIGITAL_ADOPTION_SCORE", DecimalType(5, 2)),
        _f("PRODUCT_BREADTH_INDEX", DecimalType(5, 2)),
        _f("TENURE_GROUP", StringType()),
        _f("AGE_GROUP", StringType()),
        _f("BALANCE_TIER", StringType()),
        _f("CHANNEL_PREFERENCE", StringType()),
        _f("CROSS_SELL_FLAG", StringType()),
        _f("UPSELL_FLAG", StringType()),
        _f("RETENTION_RISK_FLAG", StringType()),
        _f("MODEL_VERSION", StringType()),
        _f("EFFECTIVE_DATE", DateType()),
        _f("LOAD_TS", TimestampType()),
    ]),
    "TRANSACTION_ANALYTICS": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("REPORTING_PERIOD", StringType()),
        _f("TOTAL_ACCOUNTS", ShortType()),
        _f("ACTIVE_ACCOUNTS", ShortType()),
        _f("TOTAL_TRANSACTIONS", IntegerType()),
        _f("TOTAL_DEBIT_AMT", DecimalType(18, 2)),
        _f("TOTAL_CREDIT_AMT", DecimalType(18, 2)),
        _f("NET_CASH_FLOW", DecimalType(18, 2)),
        _f("AVG_TRANSACTION_SIZE", DecimalType(15, 2)),
        _f("MONTHLY_SPEND_TREND", StringType()),
        _f("SPEND_PERCENTILE", DecimalType(5, 2)),
        _f("TOP_SPEND_CATEGORY", StringType()),
        _f("DIGITAL_TXN_PCT", DecimalType(5, 2)),
        _f("FEE_INCOME", DecimalType(15, 2)),
        _f("INTEREST_INCOME", DecimalType(15, 2)),
        _f("REVENUE_CONTRIBUTION", DecimalType(15, 2)),
        _f("ANOMALY_FLAG", StringType()),
        _f("MODEL_VERSION", StringType()),
        _f("EFFECTIVE_DATE", DateType()),
        _f("LOAD_TS", TimestampType()),
    ]),
    "CUSTOMER_RISK_SCORES": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("COMPOSITE_RISK_SCORE", DecimalType(6, 2)),
        _f("RISK_TIER", StringType()),
        _f("PROBABILITY_OF_DEFAULT", DecimalType(7, 6)),
        _f("CREDIT_RISK_COMPONENT", DecimalType(5, 2)),
        _f("BEHAVIOUR_RISK_COMPONENT", DecimalType(5, 2)),
        _f("VELOCITY_RISK_COMPONENT", DecimalType(5, 2)),
        _f("BUREAU_SCORE_COMPONENT", DecimalType(5, 2)),
        _f("PAYMENT_HISTORY_COMPONENT", DecimalType(5, 2)),
        _f("PRIMARY_RISK_DRIVER", StringType()),
        _f("SECONDARY_RISK_DRIVER", StringType()),
        _f("SCORE_DELTA_30D", DecimalType(6, 2)),
        _f("WATCH_LIST_FLAG", StringType()),
        _f("REVIEW_REQUIRED_FLAG", StringType()),
        _f("MODEL_VERSION", StringType()),
        _f("EFFECTIVE_DATE", DateType()),
        _f("LOAD_TS", TimestampType()),
    ]),
    "CUSTOMER_MASTER_PROFILE": StructType([
        _f("CUSTOMER_ID", LongType(), False),
        _f("FULL_NAME", StringType()),
        _f("AGE", ShortType()),
        _f("STATE_CODE", StringType()),
        _f("CUSTOMER_SINCE", DateType()),
        _f("TENURE_MONTHS", IntegerType()),
        _f("CUSTOMER_STATUS", StringType()),
        _f("SEGMENT_NAME", StringType()),
        _f("LIFETIME_VALUE_SCORE", DecimalType(10, 2)),
        _f("ENGAGEMENT_SCORE", DecimalType(5, 2)),
        _f("TOTAL_ACCOUNTS", ShortType()),
        _f("ACTIVE_ACCOUNTS", ShortType()),
        _f("TOTAL_BALANCE", DecimalType(18, 2)),
        _f("TOTAL_CREDIT_LIMIT", DecimalType(18, 2)),
        _f("CREDIT_UTILIZATION_PCT", DecimalType(5, 2)),
        _f("MONTHLY_TRANSACTIONS", IntegerType()),
        _f("MONTHLY_SPEND", DecimalType(18, 2)),
        _f("NET_CASH_FLOW", DecimalType(18, 2)),
        _f("TOP_SPEND_CATEGORY", StringType()),
        _f("DIGITAL_TXN_PCT", DecimalType(5, 2)),
        _f("COMPOSITE_RISK_SCORE", DecimalType(6, 2)),
        _f("RISK_TIER", StringType()),
        _f("PROBABILITY_OF_DEFAULT", DecimalType(7, 6)),
        _f("WATCH_LIST_FLAG", StringType()),
        _f("CROSS_SELL_FLAG", StringType()),
        _f("UPSELL_FLAG", StringType()),
        _f("RETENTION_RISK_FLAG", StringType()),
        _f("MODEL_VERSION", StringType()),
        _f("EFFECTIVE_DATE", DateType()),
        _f("LOAD_TS", TimestampType()),
    ]),
}

# Audit sink replacing ETL_STAGING_DB.ETL_RUN_LOG
ETL_RUN_LOG_SCHEMA = StructType([
    _f("RUN_ID", StringType()),
    _f("JOB_NAME", StringType()),
    _f("STEP_NAME", StringType()),
    _f("STATUS", StringType()),
    _f("MESSAGE", StringType()),
    _f("ROW_COUNT", LongType()),
    _f("DURATION_SEC", DecimalType(12, 3)),
    _f("START_TS", TimestampType()),
    _f("END_TS", TimestampType()),
])

ALL_SCHEMAS: dict[str, StructType] = {**SOURCE_SCHEMAS, **SILVER_SCHEMAS, **GOLD_SCHEMAS}

# Clustering key for each managed table; replaces Teradata PRIMARY INDEX.
CLUSTER_KEYS: dict[str, list[str]] = {
    "STG_CUSTOMER_360": ["CUSTOMER_ID"],
    "STG_TXN_SUMMARY": ["CUSTOMER_ID", "ACCOUNT_ID"],
    "STG_RISK_FACTORS": ["CUSTOMER_ID"],
    "CUSTOMER_SEGMENTS": ["CUSTOMER_ID"],
    "TRANSACTION_ANALYTICS": ["CUSTOMER_ID"],
    "CUSTOMER_RISK_SCORES": ["CUSTOMER_ID"],
    "CUSTOMER_MASTER_PROFILE": ["CUSTOMER_ID"],
    "CUSTOMERS": ["CUSTOMER_ID"],
    "ACCOUNTS": ["ACCOUNT_ID"],
    "ADDRESSES": ["ADDRESS_ID"],
    "TRANSACTIONS": ["TRANSACTION_ID"],
    "CUSTOMER_BUREAU_SCORES": ["CUSTOMER_ID"],
}


def conform(df: DataFrame, schema: StructType) -> DataFrame:
    """Project ``df`` onto ``schema``: exact column set, order, and types.

    Column matching is case-insensitive so a DataFrame built with lower-case
    aliases still lands on the upper-case contract inherited from Teradata.
    """
    available = {c.lower(): c for c in df.columns}
    missing = [f.name for f in schema.fields if f.name.lower() not in available]
    if missing:
        raise ValueError(f"DataFrame is missing contract columns: {missing}")
    return df.select(
        *[
            F.col(f"`{available[f.name.lower()]}`").cast(f.dataType).alias(f.name)
            for f in schema.fields
        ]
    )


def schema_diff(actual: StructType, expected: StructType) -> list[str]:
    """Return human-readable differences between two schemas (empty == match)."""
    diffs: list[str] = []
    actual_fields = {f.name.upper(): f for f in actual.fields}
    expected_fields = {f.name.upper(): f for f in expected.fields}

    for name in expected_fields.keys() - actual_fields.keys():
        diffs.append(f"missing column {name}")
    for name in actual_fields.keys() - expected_fields.keys():
        diffs.append(f"unexpected column {name}")
    for name, exp in expected_fields.items():
        act = actual_fields.get(name)
        if act is not None and act.dataType != exp.dataType:
            diffs.append(
                f"{name}: expected {exp.dataType.simpleString()}, "
                f"found {act.dataType.simpleString()}"
            )

    actual_order = [f.name.upper() for f in actual.fields if f.name.upper() in expected_fields]
    expected_order = [f.name.upper() for f in expected.fields if f.name.upper() in actual_fields]
    if actual_order != expected_order:
        diffs.append(f"column order: expected {expected_order}, found {actual_order}")
    return diffs
