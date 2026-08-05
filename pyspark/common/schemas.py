"""DDL contracts as typed specs.

Every table in ``ddl/00_source_tables.sql``, ``ddl/01_staging_tables.sql`` and
``ddl/02_data_product_tables.sql`` is transcribed here, column for column, in DDL order, with
its Teradata type mapped to a Spark type, its ``DEFAULT`` clause, its primary index and its
partitioning. Jobs must never hand-build a schema: they call :func:`enforce_schema` before
writing and the functional tests call :func:`assert_schema`.

Two audit tables (``ETL_RUN_LOG``, ``PIPELINE_AUDIT``) and one source table
(``CUSTOMER_BUREAU_SCORES``) have no DDL in the repository; their specs are inferred from the
INSERT statements in the BTEQ scripts, from ``%init_audit`` and from the committed source CSV.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
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


class SchemaMismatchError(AssertionError):
    """Raised when a DataFrame does not match its DDL contract."""


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: DataType
    nullable: bool = True
    default: str | int | float | None = None
    comment: str | None = None


@dataclass(frozen=True)
class TableSpec:
    """A Teradata table definition expressed as a Spark contract."""

    database: str
    name: str
    columns: tuple[ColumnSpec, ...]
    primary_index: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    source: str = ""
    inferred: bool = False
    _by_name: dict[str, ColumnSpec] = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._by_name.update({column.name: column for column in self.columns})

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def column(self, name: str) -> ColumnSpec:
        try:
            return self._by_name[name]
        except KeyError as exc:
            raise KeyError(f"{self.qualified_name} has no column {name}") from exc

    def spark_schema(self) -> StructType:
        return StructType(
            [StructField(column.name, column.dtype, column.nullable) for column in self.columns]
        )


def _cast(column: Column, dtype: DataType) -> Column:
    """Cast widening enough to survive messy CSV inputs.

    Integer columns are frequently written as ``3.0`` by pandas exports; a direct
    ``cast("int")`` on that string yields NULL, so integral targets go via ``double`` first.
    """

    if isinstance(dtype, (ShortType, IntegerType, LongType)):
        return column.cast("double").cast(dtype)
    return column.cast(dtype)


def enforce_schema(df: DataFrame, spec: TableSpec, *, allow_missing: bool = False) -> DataFrame:
    """Project ``df`` onto ``spec``: DDL column order, DDL types, DDL defaults.

    Missing columns are only tolerated when ``allow_missing`` is set, in which case they are
    materialised as NULL (or as their DDL default).
    """

    available = set(df.columns)
    projected: list[Column] = []
    for column in spec.columns:
        if column.name in available:
            value = _cast(F.col(f"`{column.name}`"), column.dtype)
        elif allow_missing or column.default is not None:
            value = F.lit(None).cast(column.dtype)
        else:
            raise SchemaMismatchError(
                f"{spec.qualified_name}: column {column.name} is missing from the DataFrame"
            )
        if column.default is not None:
            value = F.coalesce(value, F.lit(column.default).cast(column.dtype))
        projected.append(value.alias(column.name))
    return df.select(*projected)


def assert_schema(df: DataFrame, spec: TableSpec) -> None:
    """Assert an exact match against the DDL contract: names, order and types."""

    actual = [(field_.name, field_.dataType) for field_ in df.schema.fields]
    expected = [(column.name, column.dtype) for column in spec.columns]
    if actual != expected:
        actual_names = [name for name, _ in actual]
        expected_names = [name for name, _ in expected]
        if actual_names != expected_names:
            raise SchemaMismatchError(
                f"{spec.qualified_name}: column mismatch\n  expected: {expected_names}\n  actual:   {actual_names}"
            )
        diffs = [
            f"{name}: expected {exp_type.simpleString()}, got {act_type.simpleString()}"
            for (name, act_type), (_, exp_type) in zip(actual, expected, strict=False)
            if act_type != exp_type
        ]
        raise SchemaMismatchError(f"{spec.qualified_name}: type mismatch\n  " + "\n  ".join(diffs))


# --------------------------------------------------------------------------------------------
# Source tables - ddl/00_source_tables.sql
# --------------------------------------------------------------------------------------------

CUSTOMERS = TableSpec(
    database="CORE_BANKING_DB",
    name="CUSTOMERS",
    source="ddl/00_source_tables.sql",
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("FIRST_NAME", StringType()),
        ColumnSpec("LAST_NAME", StringType()),
        ColumnSpec("DATE_OF_BIRTH", DateType()),
        ColumnSpec("SSN_HASH", StringType()),
        ColumnSpec("EMAIL", StringType()),
        ColumnSpec("PHONE_PRIMARY", StringType()),
        ColumnSpec("CUSTOMER_SINCE", DateType()),
        ColumnSpec("CUSTOMER_STATUS", StringType(), comment="A=Active, I=Inactive, C=Closed"),
        ColumnSpec("SEGMENT_CODE", StringType()),
        ColumnSpec("BRANCH_ID", IntegerType()),
        ColumnSpec("CREATED_TS", TimestampType()),
        ColumnSpec("UPDATED_TS", TimestampType()),
    ),
)

ACCOUNTS = TableSpec(
    database="CORE_BANKING_DB",
    name="ACCOUNTS",
    source="ddl/00_source_tables.sql",
    primary_index=("ACCOUNT_ID",),
    columns=(
        ColumnSpec("ACCOUNT_ID", LongType(), nullable=False),
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("ACCOUNT_TYPE", StringType(), comment="CHECKING, SAVINGS, CREDIT, LOAN"),
        ColumnSpec("ACCOUNT_STATUS", StringType(), comment="O=Open, C=Closed, F=Frozen"),
        ColumnSpec("OPEN_DATE", DateType()),
        ColumnSpec("CLOSE_DATE", DateType()),
        ColumnSpec("CURRENT_BALANCE", DecimalType(15, 2)),
        ColumnSpec("AVAILABLE_BALANCE", DecimalType(15, 2)),
        ColumnSpec("CREDIT_LIMIT", DecimalType(15, 2)),
        ColumnSpec("INTEREST_RATE", DecimalType(5, 4)),
        ColumnSpec("BRANCH_ID", IntegerType()),
        ColumnSpec("CREATED_TS", TimestampType()),
        ColumnSpec("UPDATED_TS", TimestampType()),
    ),
)

ADDRESSES = TableSpec(
    database="CORE_BANKING_DB",
    name="ADDRESSES",
    source="ddl/00_source_tables.sql",
    primary_index=("ADDRESS_ID",),
    columns=(
        ColumnSpec("ADDRESS_ID", LongType(), nullable=False),
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("ADDRESS_TYPE", StringType(), comment="MAIL, HOME, WORK"),
        ColumnSpec("ADDRESS_LINE_1", StringType()),
        ColumnSpec("ADDRESS_LINE_2", StringType()),
        ColumnSpec("CITY", StringType()),
        ColumnSpec("STATE_CODE", StringType()),
        ColumnSpec("ZIP_CODE", StringType()),
        ColumnSpec("COUNTRY_CODE", StringType(), default="US"),
        ColumnSpec("IS_PRIMARY", StringType(), default="N"),
        ColumnSpec("EFFECTIVE_DATE", DateType()),
        ColumnSpec("EXPIRATION_DATE", DateType()),
        ColumnSpec("CREATED_TS", TimestampType()),
        ColumnSpec("UPDATED_TS", TimestampType()),
    ),
)

TRANSACTIONS = TableSpec(
    database="TXN_PROCESSING_DB",
    name="TRANSACTIONS",
    source="ddl/00_source_tables.sql",
    primary_index=("TRANSACTION_ID",),
    partition_by=("TRANSACTION_DATE",),
    columns=(
        ColumnSpec("TRANSACTION_ID", LongType(), nullable=False),
        ColumnSpec("ACCOUNT_ID", LongType(), nullable=False),
        ColumnSpec("TRANSACTION_TYPE_CD", StringType()),
        ColumnSpec("TRANSACTION_DATE", DateType()),
        ColumnSpec("TRANSACTION_TS", TimestampType()),
        ColumnSpec("AMOUNT", DecimalType(15, 2)),
        ColumnSpec("RUNNING_BALANCE", DecimalType(15, 2)),
        ColumnSpec("MERCHANT_NAME", StringType()),
        ColumnSpec("MERCHANT_CATEGORY", StringType()),
        ColumnSpec("CHANNEL_CODE", StringType(), comment="ATM, POS, WEB, MOB, ACH, WIRE"),
        ColumnSpec("REFERENCE_NUM", StringType()),
        ColumnSpec("STATUS_CODE", StringType(), comment="P=Posted, R=Reversed, H=Hold"),
        ColumnSpec("CREATED_TS", TimestampType()),
    ),
)

TRANSACTION_TYPES = TableSpec(
    database="TXN_PROCESSING_DB",
    name="TRANSACTION_TYPES",
    source="ddl/00_source_tables.sql",
    primary_index=("TRANSACTION_TYPE_CD",),
    columns=(
        ColumnSpec("TRANSACTION_TYPE_CD", StringType(), nullable=False),
        ColumnSpec("DESCRIPTION", StringType()),
        ColumnSpec("CATEGORY", StringType(), comment="DEBIT, CREDIT, FEE, INTEREST"),
        ColumnSpec("IS_REVENUE", StringType(), default="N"),
        ColumnSpec("EFFECTIVE_DATE", DateType()),
        ColumnSpec("EXPIRATION_DATE", DateType()),
    ),
)

CUSTOMER_BUREAU_SCORES = TableSpec(
    database="CORE_BANKING_DB",
    name="CUSTOMER_BUREAU_SCORES",
    source="inferred from bteq/03_stg_risk_factors.bteq + data/01_source_tables/customer_bureau_scores.csv",
    inferred=True,
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("EXTERNAL_CREDIT_SCORE", IntegerType()),
        ColumnSpec("REPORT_DATE", DateType()),
    ),
)

# --------------------------------------------------------------------------------------------
# Staging tables - ddl/01_staging_tables.sql
# --------------------------------------------------------------------------------------------

STG_CUSTOMER_360 = TableSpec(
    database="ETL_STAGING_DB",
    name="STG_CUSTOMER_360",
    source="ddl/01_staging_tables.sql",
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("FIRST_NAME", StringType()),
        ColumnSpec("LAST_NAME", StringType()),
        ColumnSpec("DATE_OF_BIRTH", DateType()),
        ColumnSpec("AGE", ShortType()),
        ColumnSpec("CUSTOMER_SINCE", DateType()),
        ColumnSpec("TENURE_MONTHS", IntegerType()),
        ColumnSpec("CUSTOMER_STATUS", StringType()),
        ColumnSpec("SEGMENT_CODE", StringType()),
        ColumnSpec("BRANCH_ID", IntegerType()),
        ColumnSpec("PRIMARY_ADDRESS", StringType()),
        ColumnSpec("CITY", StringType()),
        ColumnSpec("STATE_CODE", StringType()),
        ColumnSpec("ZIP_CODE", StringType()),
        ColumnSpec("NUM_ACCOUNTS", ShortType()),
        ColumnSpec("NUM_ACTIVE_ACCOUNTS", ShortType()),
        ColumnSpec("HAS_CHECKING", StringType(), default="N"),
        ColumnSpec("HAS_SAVINGS", StringType(), default="N"),
        ColumnSpec("HAS_CREDIT", StringType(), default="N"),
        ColumnSpec("HAS_LOAN", StringType(), default="N"),
        ColumnSpec("TOTAL_BALANCE", DecimalType(18, 2)),
        ColumnSpec("TOTAL_CREDIT_LIMIT", DecimalType(18, 2)),
        ColumnSpec("CREDIT_UTILIZATION_PCT", DecimalType(5, 2)),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)

STG_TXN_SUMMARY = TableSpec(
    database="ETL_STAGING_DB",
    name="STG_TXN_SUMMARY",
    source="ddl/01_staging_tables.sql",
    primary_index=("CUSTOMER_ID", "ACCOUNT_ID"),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("ACCOUNT_ID", LongType(), nullable=False),
        ColumnSpec("ACCOUNT_TYPE", StringType()),
        ColumnSpec("SUMMARY_PERIOD_START", DateType()),
        ColumnSpec("SUMMARY_PERIOD_END", DateType()),
        ColumnSpec("TXN_COUNT_TOTAL", IntegerType()),
        ColumnSpec("TXN_COUNT_DEBIT", IntegerType()),
        ColumnSpec("TXN_COUNT_CREDIT", IntegerType()),
        ColumnSpec("TXN_COUNT_FEE", IntegerType()),
        ColumnSpec("AMT_TOTAL_DEBIT", DecimalType(18, 2)),
        ColumnSpec("AMT_TOTAL_CREDIT", DecimalType(18, 2)),
        ColumnSpec("AMT_TOTAL_FEES", DecimalType(18, 2)),
        ColumnSpec("AMT_AVG_DEBIT", DecimalType(15, 2)),
        ColumnSpec("AMT_AVG_CREDIT", DecimalType(15, 2)),
        ColumnSpec("AMT_MAX_SINGLE_DEBIT", DecimalType(15, 2)),
        ColumnSpec("AMT_MAX_SINGLE_CREDIT", DecimalType(15, 2)),
        ColumnSpec("DISTINCT_MERCHANTS", IntegerType()),
        ColumnSpec("TOP_MERCHANT_CATEGORY", StringType()),
        ColumnSpec("PCT_ATM", DecimalType(5, 2)),
        ColumnSpec("PCT_POS", DecimalType(5, 2)),
        ColumnSpec("PCT_WEB", DecimalType(5, 2)),
        ColumnSpec("PCT_MOBILE", DecimalType(5, 2)),
        ColumnSpec("DAYS_SINCE_LAST_TXN", IntegerType()),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)

STG_RISK_FACTORS = TableSpec(
    database="ETL_STAGING_DB",
    name="STG_RISK_FACTORS",
    source="ddl/01_staging_tables.sql",
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("ACCOUNT_OVERDRAFT_CNT", IntegerType()),
        ColumnSpec("NSF_FEE_TOTAL", DecimalType(15, 2)),
        ColumnSpec("LARGE_WITHDRAWAL_CNT", IntegerType()),
        ColumnSpec("LARGE_WITHDRAWAL_AMT", DecimalType(18, 2)),
        ColumnSpec("AVG_DAILY_BALANCE_30D", DecimalType(15, 2)),
        ColumnSpec("AVG_DAILY_BALANCE_90D", DecimalType(15, 2)),
        ColumnSpec("BALANCE_VOLATILITY", DecimalType(10, 4)),
        ColumnSpec("CREDIT_UTIL_RATIO", DecimalType(5, 4)),
        ColumnSpec("PAYMENT_ONTIME_PCT", DecimalType(5, 2)),
        ColumnSpec("PAYMENT_LATE_CNT", IntegerType()),
        ColumnSpec("MONTHS_SINCE_LAST_LATE", IntegerType()),
        ColumnSpec("EXTERNAL_CREDIT_SCORE", IntegerType()),
        ColumnSpec("DEBIT_VELOCITY_7D", DecimalType(15, 2)),
        ColumnSpec("DEBIT_VELOCITY_30D", DecimalType(15, 2)),
        ColumnSpec("NEW_MERCHANT_CNT_30D", IntegerType()),
        ColumnSpec("INTERNATIONAL_TXN_CNT", IntegerType()),
        ColumnSpec("HIGH_RISK_MERCHANT_CNT", IntegerType()),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)

ETL_RUN_LOG = TableSpec(
    database="ETL_STAGING_DB",
    name="ETL_RUN_LOG",
    source="inferred from the INSERT statements in bteq/0*.bteq",
    inferred=True,
    primary_index=("JOB_NAME",),
    columns=(
        ColumnSpec("JOB_NAME", StringType()),
        ColumnSpec("STEP_NAME", StringType()),
        ColumnSpec("STATUS", StringType()),
        ColumnSpec("ROW_COUNT", LongType()),
        ColumnSpec("START_TS", TimestampType()),
        ColumnSpec("END_TS", TimestampType()),
    ),
)

PIPELINE_AUDIT = TableSpec(
    database="ETL_STAGING_DB",
    name="PIPELINE_AUDIT",
    source="inferred from sas/macros/log_step.sas (%init_audit / %log_step)",
    inferred=True,
    primary_index=("JOB_NAME",),
    columns=(
        ColumnSpec("JOB_NAME", StringType()),
        ColumnSpec("STATUS", StringType()),
        ColumnSpec("MESSAGE", StringType()),
        ColumnSpec("ROW_COUNT", LongType()),
        ColumnSpec("LOG_TS", TimestampType()),
    ),
)

# --------------------------------------------------------------------------------------------
# Data products - ddl/02_data_product_tables.sql
# --------------------------------------------------------------------------------------------

CUSTOMER_SEGMENTS = TableSpec(
    database="DATA_PRODUCTS_DB",
    name="CUSTOMER_SEGMENTS",
    source="ddl/02_data_product_tables.sql",
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("SEGMENT_NAME", StringType()),
        ColumnSpec("SEGMENT_ID", ShortType()),
        ColumnSpec("SUBSEGMENT_ID", ShortType()),
        ColumnSpec("LIFETIME_VALUE_SCORE", DecimalType(10, 2)),
        ColumnSpec("ENGAGEMENT_SCORE", DecimalType(5, 2)),
        ColumnSpec("DIGITAL_ADOPTION_SCORE", DecimalType(5, 2)),
        ColumnSpec("PRODUCT_BREADTH_INDEX", DecimalType(5, 2)),
        ColumnSpec("TENURE_GROUP", StringType()),
        ColumnSpec("AGE_GROUP", StringType()),
        ColumnSpec("BALANCE_TIER", StringType()),
        ColumnSpec("CHANNEL_PREFERENCE", StringType()),
        ColumnSpec("CROSS_SELL_FLAG", StringType(), default="N"),
        ColumnSpec("UPSELL_FLAG", StringType(), default="N"),
        ColumnSpec("RETENTION_RISK_FLAG", StringType(), default="N"),
        ColumnSpec("MODEL_VERSION", StringType()),
        ColumnSpec("EFFECTIVE_DATE", DateType()),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)

TRANSACTION_ANALYTICS = TableSpec(
    database="DATA_PRODUCTS_DB",
    name="TRANSACTION_ANALYTICS",
    source="ddl/02_data_product_tables.sql",
    primary_index=("CUSTOMER_ID",),
    partition_by=("REPORTING_PERIOD",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("REPORTING_PERIOD", StringType(), comment="YYYY-MM"),
        ColumnSpec("TOTAL_ACCOUNTS", ShortType()),
        ColumnSpec("ACTIVE_ACCOUNTS", ShortType()),
        ColumnSpec("TOTAL_TRANSACTIONS", IntegerType()),
        ColumnSpec("TOTAL_DEBIT_AMT", DecimalType(18, 2)),
        ColumnSpec("TOTAL_CREDIT_AMT", DecimalType(18, 2)),
        ColumnSpec("NET_CASH_FLOW", DecimalType(18, 2)),
        ColumnSpec("AVG_TRANSACTION_SIZE", DecimalType(15, 2)),
        ColumnSpec("MONTHLY_SPEND_TREND", StringType(), comment="UP, DOWN, STABLE"),
        ColumnSpec("SPEND_PERCENTILE", DecimalType(5, 2)),
        ColumnSpec("TOP_SPEND_CATEGORY", StringType()),
        ColumnSpec("DIGITAL_TXN_PCT", DecimalType(5, 2)),
        ColumnSpec("FEE_INCOME", DecimalType(15, 2)),
        ColumnSpec("INTEREST_INCOME", DecimalType(15, 2)),
        ColumnSpec("REVENUE_CONTRIBUTION", DecimalType(15, 2)),
        ColumnSpec("ANOMALY_FLAG", StringType(), default="N"),
        ColumnSpec("MODEL_VERSION", StringType()),
        ColumnSpec("EFFECTIVE_DATE", DateType()),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)

CUSTOMER_RISK_SCORES = TableSpec(
    database="DATA_PRODUCTS_DB",
    name="CUSTOMER_RISK_SCORES",
    source="ddl/02_data_product_tables.sql",
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("COMPOSITE_RISK_SCORE", DecimalType(6, 2)),
        ColumnSpec("RISK_TIER", StringType(), comment="LOW, MODERATE, ELEVATED, HIGH, CRITICAL"),
        ColumnSpec("PROBABILITY_OF_DEFAULT", DecimalType(7, 6)),
        ColumnSpec("CREDIT_RISK_COMPONENT", DecimalType(5, 2)),
        ColumnSpec("BEHAVIOUR_RISK_COMPONENT", DecimalType(5, 2)),
        ColumnSpec("VELOCITY_RISK_COMPONENT", DecimalType(5, 2)),
        ColumnSpec("BUREAU_SCORE_COMPONENT", DecimalType(5, 2)),
        ColumnSpec("PAYMENT_HISTORY_COMPONENT", DecimalType(5, 2)),
        ColumnSpec("PRIMARY_RISK_DRIVER", StringType()),
        ColumnSpec("SECONDARY_RISK_DRIVER", StringType()),
        ColumnSpec("SCORE_DELTA_30D", DecimalType(6, 2)),
        ColumnSpec("WATCH_LIST_FLAG", StringType(), default="N"),
        ColumnSpec("REVIEW_REQUIRED_FLAG", StringType(), default="N"),
        ColumnSpec("MODEL_VERSION", StringType()),
        ColumnSpec("EFFECTIVE_DATE", DateType()),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)

CUSTOMER_MASTER_PROFILE = TableSpec(
    database="DATA_PRODUCTS_DB",
    name="CUSTOMER_MASTER_PROFILE",
    source="ddl/02_data_product_tables.sql",
    primary_index=("CUSTOMER_ID",),
    columns=(
        ColumnSpec("CUSTOMER_ID", LongType(), nullable=False),
        ColumnSpec("FULL_NAME", StringType()),
        ColumnSpec("AGE", ShortType()),
        ColumnSpec("STATE_CODE", StringType()),
        ColumnSpec("CUSTOMER_SINCE", DateType()),
        ColumnSpec("TENURE_MONTHS", IntegerType()),
        ColumnSpec("CUSTOMER_STATUS", StringType()),
        ColumnSpec("SEGMENT_NAME", StringType()),
        ColumnSpec("LIFETIME_VALUE_SCORE", DecimalType(10, 2)),
        ColumnSpec("ENGAGEMENT_SCORE", DecimalType(5, 2)),
        ColumnSpec("TOTAL_ACCOUNTS", ShortType()),
        ColumnSpec("ACTIVE_ACCOUNTS", ShortType()),
        ColumnSpec("TOTAL_BALANCE", DecimalType(18, 2)),
        ColumnSpec("TOTAL_CREDIT_LIMIT", DecimalType(18, 2)),
        ColumnSpec("CREDIT_UTILIZATION_PCT", DecimalType(5, 2)),
        ColumnSpec("MONTHLY_TRANSACTIONS", IntegerType()),
        ColumnSpec("MONTHLY_SPEND", DecimalType(18, 2)),
        ColumnSpec("NET_CASH_FLOW", DecimalType(18, 2)),
        ColumnSpec("TOP_SPEND_CATEGORY", StringType()),
        ColumnSpec("DIGITAL_TXN_PCT", DecimalType(5, 2)),
        ColumnSpec("COMPOSITE_RISK_SCORE", DecimalType(6, 2)),
        ColumnSpec("RISK_TIER", StringType()),
        ColumnSpec("PROBABILITY_OF_DEFAULT", DecimalType(7, 6)),
        ColumnSpec("WATCH_LIST_FLAG", StringType(), default="N"),
        ColumnSpec("CROSS_SELL_FLAG", StringType(), default="N"),
        ColumnSpec("UPSELL_FLAG", StringType(), default="N"),
        ColumnSpec("RETENTION_RISK_FLAG", StringType(), default="N"),
        ColumnSpec("MODEL_VERSION", StringType()),
        ColumnSpec("EFFECTIVE_DATE", DateType()),
        ColumnSpec("LOAD_TS", TimestampType()),
    ),
)


ALL_SPECS: tuple[TableSpec, ...] = (
    CUSTOMERS,
    ACCOUNTS,
    ADDRESSES,
    TRANSACTIONS,
    TRANSACTION_TYPES,
    CUSTOMER_BUREAU_SCORES,
    STG_CUSTOMER_360,
    STG_TXN_SUMMARY,
    STG_RISK_FACTORS,
    ETL_RUN_LOG,
    PIPELINE_AUDIT,
    CUSTOMER_SEGMENTS,
    TRANSACTION_ANALYTICS,
    CUSTOMER_RISK_SCORES,
    CUSTOMER_MASTER_PROFILE,
)

SPECS_BY_TABLE: dict[str, TableSpec] = {spec.name: spec for spec in ALL_SPECS}
SPECS_BY_QUALIFIED_NAME: dict[str, TableSpec] = {spec.qualified_name: spec for spec in ALL_SPECS}


def spec_for(database: str, table: str) -> TableSpec:
    """Look up a contract by database and table name."""

    try:
        return SPECS_BY_QUALIFIED_NAME[f"{database.upper()}.{table.upper()}"]
    except KeyError as exc:
        raise KeyError(f"no DDL contract registered for {database}.{table}") from exc
