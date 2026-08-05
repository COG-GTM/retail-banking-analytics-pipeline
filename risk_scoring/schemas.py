"""Shared column names and schemas — the interface contract between modules.

Every DataFrame handed between modules uses UPPER_SNAKE column names, matching
the Teradata DDL and the SAS variable names. The CSV backend lower-cases its
headers on export, so readers normalise back to upper case.

Types follow ``ddl/01_staging_tables.sql`` and ``ddl/02_data_product_tables.sql``.
Intermediate DataFrames stay in ``double`` for the arithmetic; the DECIMAL casts
of the target schema are applied once, at the sink.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Source tables (ETL_STAGING_DB)                                               #
# --------------------------------------------------------------------------- #

STG_RISK_FACTORS = "STG_RISK_FACTORS"
STG_CUSTOMER_360 = "STG_CUSTOMER_360"
CUSTOMER_RISK_SCORES = "CUSTOMER_RISK_SCORES"

STG_RISK_FACTORS_SCHEMA = StructType([
    StructField("CUSTOMER_ID", LongType(), False),
    StructField("ACCOUNT_OVERDRAFT_CNT", IntegerType(), True),
    StructField("NSF_FEE_TOTAL", DecimalType(15, 2), True),
    StructField("LARGE_WITHDRAWAL_CNT", IntegerType(), True),
    StructField("LARGE_WITHDRAWAL_AMT", DecimalType(18, 2), True),
    StructField("AVG_DAILY_BALANCE_30D", DecimalType(15, 2), True),
    StructField("AVG_DAILY_BALANCE_90D", DecimalType(15, 2), True),
    StructField("BALANCE_VOLATILITY", DecimalType(10, 4), True),
    StructField("CREDIT_UTIL_RATIO", DecimalType(5, 4), True),
    StructField("PAYMENT_ONTIME_PCT", DecimalType(5, 2), True),
    StructField("PAYMENT_LATE_CNT", IntegerType(), True),
    StructField("MONTHS_SINCE_LAST_LATE", IntegerType(), True),
    StructField("EXTERNAL_CREDIT_SCORE", IntegerType(), True),
    StructField("DEBIT_VELOCITY_7D", DecimalType(15, 2), True),
    StructField("DEBIT_VELOCITY_30D", DecimalType(15, 2), True),
    StructField("NEW_MERCHANT_CNT_30D", IntegerType(), True),
    StructField("INTERNATIONAL_TXN_CNT", IntegerType(), True),
    StructField("HIGH_RISK_MERCHANT_CNT", IntegerType(), True),
    StructField("LOAD_TS", TimestampType(), True),
])

# Only the four columns 03_sas_risk_scoring.sas selects from STG_CUSTOMER_360
# (plus the join key) are part of the contract.
STG_CUSTOMER_360_RISK_COLUMNS = (
    "CUSTOMER_ID",
    "TENURE_MONTHS",
    "NUM_ACTIVE_ACCOUNTS",
    "TOTAL_BALANCE",
    "CUSTOMER_STATUS",
)

# --------------------------------------------------------------------------- #
# Intermediate contracts                                                       #
# --------------------------------------------------------------------------- #

#: Columns added by ``ingestion.build_risk_features`` (WORK.RISK_FEATURES).
RISK_FEATURE_COLUMNS = (
    "BUREAU_SCORE_NORM",
    "BALANCE_TREND_RATIO",
    "VELOCITY_RATIO",
    "DEFAULT_FLAG",
)

#: Predictors of the SAS PROC LOGISTIC model, in MODEL statement order.
MODEL_PREDICTORS = (
    "BUREAU_SCORE_NORM",
    "CREDIT_UTIL_RATIO",
    "PAYMENT_ONTIME_PCT",
    "BALANCE_VOLATILITY",
    "VELOCITY_RATIO",
    "ACCOUNT_OVERDRAFT_CNT",
    "LARGE_WITHDRAWAL_CNT",
    "HIGH_RISK_MERCHANT_CNT",
    "TENURE_MONTHS",
)

MODEL_TARGET = "DEFAULT_FLAG"

#: Column added by ``model.train_and_score`` (WORK.RISK_SCORED).
PROB_DEFAULT = "PROB_DEFAULT"

#: Risk driver labels, in SAS ``_lbl`` array order. Index order is significant:
#: ties resolve to the lowest index.
RISK_DRIVER_LABELS = (
    "CREDIT_UTILIZATION",
    "PAYMENT_BEHAVIOUR",
    "TRANSACTION_VELOCITY",
    "BUREAU_SCORE",
)

# --------------------------------------------------------------------------- #
# Target table (DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES)                         #
# --------------------------------------------------------------------------- #

CUSTOMER_RISK_SCORES_SCHEMA = StructType([
    StructField("CUSTOMER_ID", LongType(), False),
    StructField("COMPOSITE_RISK_SCORE", DecimalType(6, 2), True),
    StructField("RISK_TIER", StringType(), True),
    StructField("PROBABILITY_OF_DEFAULT", DecimalType(7, 6), True),
    StructField("CREDIT_RISK_COMPONENT", DecimalType(5, 2), True),
    StructField("BEHAVIOUR_RISK_COMPONENT", DecimalType(5, 2), True),
    StructField("VELOCITY_RISK_COMPONENT", DecimalType(5, 2), True),
    StructField("BUREAU_SCORE_COMPONENT", DecimalType(5, 2), True),
    StructField("PAYMENT_HISTORY_COMPONENT", DecimalType(5, 2), True),
    StructField("PRIMARY_RISK_DRIVER", StringType(), True),
    StructField("SECONDARY_RISK_DRIVER", StringType(), True),
    StructField("SCORE_DELTA_30D", DecimalType(6, 2), True),
    StructField("WATCH_LIST_FLAG", StringType(), True),
    StructField("REVIEW_REQUIRED_FLAG", StringType(), True),
    StructField("MODEL_VERSION", StringType(), True),
    StructField("EFFECTIVE_DATE", DateType(), True),
    StructField("LOAD_TS", TimestampType(), True),
])

#: Target column order, used by the sink to project before writing.
CUSTOMER_RISK_SCORES_COLUMNS = tuple(f.name for f in CUSTOMER_RISK_SCORES_SCHEMA.fields)

#: Fields that must match the oracle exactly (deterministic, model-independent).
DETERMINISTIC_PARITY_COLUMNS = (
    "COMPOSITE_RISK_SCORE",
    "RISK_TIER",
    "CREDIT_RISK_COMPONENT",
    "BEHAVIOUR_RISK_COMPONENT",
    "VELOCITY_RISK_COMPONENT",
    "BUREAU_SCORE_COMPONENT",
    "PAYMENT_HISTORY_COMPONENT",
    "PRIMARY_RISK_DRIVER",
    "SECONDARY_RISK_DRIVER",
    "SCORE_DELTA_30D",
    "REVIEW_REQUIRED_FLAG",
)

#: Fields compared only at distribution level (logistic model is not reproducible).
APPROXIMATE_PARITY_COLUMNS = ("PROBABILITY_OF_DEFAULT", "WATCH_LIST_FLAG")

_ANALYTIC_DOUBLE_COLUMNS = (
    "NSF_FEE_TOTAL",
    "LARGE_WITHDRAWAL_AMT",
    "AVG_DAILY_BALANCE_30D",
    "AVG_DAILY_BALANCE_90D",
    "BALANCE_VOLATILITY",
    "CREDIT_UTIL_RATIO",
    "PAYMENT_ONTIME_PCT",
    "DEBIT_VELOCITY_7D",
    "DEBIT_VELOCITY_30D",
    "TOTAL_BALANCE",
)


def analytic_schema(schema: StructType) -> StructType:
    """Return ``schema`` with DECIMAL measures widened to DOUBLE.

    SAS holds every numeric variable as a 64-bit float, so the arithmetic in
    STEP 2 and STEP 4 runs in double precision. Reading the staging tables as
    DECIMAL would silently truncate intermediate ratios.
    """
    return StructType([
        StructField(f.name, DoubleType(), f.nullable)
        if f.name in _ANALYTIC_DOUBLE_COLUMNS
        else f
        for f in schema.fields
    ])
