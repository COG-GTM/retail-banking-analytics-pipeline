"""Output contracts ported 1:1 from the DDL.

The three ``ddl/*.sql`` files are the authoritative column/type/default/partition
contract.  Each table here reproduces the DDL column order, Spark-mapped types,
NOT NULL flags, DEFAULT literals and partition columns so every job can enforce
the exact same schema the legacy Teradata tables exposed to downstream
consumers.

Teradata -> Spark type map used throughout:
    BIGINT      -> LongType
    INTEGER     -> IntegerType
    SMALLINT    -> ShortType
    DECIMAL(p,s)-> DecimalType(p, s)
    VARCHAR/CHAR-> StringType
    DATE        -> DateType
    TIMESTAMP(6)-> TimestampType
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

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


@dataclass(frozen=True)
class Column:
    name: str
    dtype: object
    nullable: bool = True
    default: object = None


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    primary_index: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()

    @property
    def struct(self) -> StructType:
        return StructType(
            [StructField(c.name, c.dtype, c.nullable) for c in self.columns]
        )

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def defaults(self) -> dict[str, object]:
        return {c.name: c.default for c in self.columns if c.default is not None}


def _T(name, cols, pi=(), part=()):
    return TableSpec(name, tuple(cols), tuple(pi), tuple(part))


# --------------------------------------------------------------------------- #
# Source tables (ddl/00_source_tables.sql) -- used to type the CSV/lake reads. #
# --------------------------------------------------------------------------- #
CUSTOMERS = _T("CUSTOMERS", [
    Column("customer_id", LongType(), nullable=False),
    Column("first_name", StringType()),
    Column("last_name", StringType()),
    Column("date_of_birth", DateType()),
    Column("ssn_hash", StringType()),
    Column("email", StringType()),
    Column("phone_primary", StringType()),
    Column("customer_since", DateType()),
    Column("customer_status", StringType()),
    Column("segment_code", StringType()),
    Column("branch_id", IntegerType()),
    Column("created_ts", TimestampType()),
    Column("updated_ts", TimestampType()),
], pi=("customer_id",))

ACCOUNTS = _T("ACCOUNTS", [
    Column("account_id", LongType(), nullable=False),
    Column("customer_id", LongType(), nullable=False),
    Column("account_type", StringType()),
    Column("account_status", StringType()),
    Column("open_date", DateType()),
    Column("close_date", DateType()),
    Column("current_balance", DecimalType(15, 2)),
    Column("available_balance", DecimalType(15, 2)),
    Column("credit_limit", DecimalType(15, 2)),
    Column("interest_rate", DecimalType(5, 4)),
    Column("branch_id", IntegerType()),
    Column("created_ts", TimestampType()),
    Column("updated_ts", TimestampType()),
], pi=("account_id",))

ADDRESSES = _T("ADDRESSES", [
    Column("address_id", LongType(), nullable=False),
    Column("customer_id", LongType(), nullable=False),
    Column("address_type", StringType()),
    Column("address_line_1", StringType()),
    Column("address_line_2", StringType()),
    Column("city", StringType()),
    Column("state_code", StringType()),
    Column("zip_code", StringType()),
    Column("country_code", StringType(), default="US"),
    Column("is_primary", StringType(), default="N"),
    Column("effective_date", DateType()),
    Column("expiration_date", DateType()),
    Column("created_ts", TimestampType()),
    Column("updated_ts", TimestampType()),
], pi=("address_id",))

TRANSACTIONS = _T("TRANSACTIONS", [
    Column("transaction_id", LongType(), nullable=False),
    Column("account_id", LongType(), nullable=False),
    Column("transaction_type_cd", StringType()),
    Column("transaction_date", DateType()),
    Column("transaction_ts", TimestampType()),
    Column("amount", DecimalType(15, 2)),
    Column("running_balance", DecimalType(15, 2)),
    Column("merchant_name", StringType()),
    Column("merchant_category", StringType()),
    Column("channel_code", StringType()),
    Column("reference_num", StringType()),
    Column("status_code", StringType()),
    Column("created_ts", TimestampType()),
], pi=("transaction_id",))

TRANSACTION_TYPES = _T("TRANSACTION_TYPES", [
    Column("transaction_type_cd", StringType(), nullable=False),
    Column("description", StringType()),
    Column("category", StringType()),
    Column("is_revenue", StringType(), default="N"),
    Column("effective_date", DateType()),
    Column("expiration_date", DateType()),
], pi=("transaction_type_cd",))

CUSTOMER_BUREAU_SCORES = _T("CUSTOMER_BUREAU_SCORES", [
    Column("customer_id", LongType(), nullable=False),
    Column("external_credit_score", IntegerType()),
    Column("report_date", DateType()),
])

SOURCE_TABLES = {
    t.name: t for t in (
        CUSTOMERS, ACCOUNTS, ADDRESSES, TRANSACTIONS,
        TRANSACTION_TYPES, CUSTOMER_BUREAU_SCORES,
    )
}

# --------------------------------------------------------------------------- #
# Staging tables (ddl/01_staging_tables.sql)                                   #
# --------------------------------------------------------------------------- #
STG_CUSTOMER_360 = _T("STG_CUSTOMER_360", [
    Column("customer_id", LongType(), nullable=False),
    Column("first_name", StringType()),
    Column("last_name", StringType()),
    Column("date_of_birth", DateType()),
    Column("age", ShortType()),
    Column("customer_since", DateType()),
    Column("tenure_months", IntegerType()),
    Column("customer_status", StringType()),
    Column("segment_code", StringType()),
    Column("branch_id", IntegerType()),
    Column("primary_address", StringType()),
    Column("city", StringType()),
    Column("state_code", StringType()),
    Column("zip_code", StringType()),
    Column("num_accounts", ShortType()),
    Column("num_active_accounts", ShortType()),
    Column("has_checking", StringType(), default="N"),
    Column("has_savings", StringType(), default="N"),
    Column("has_credit", StringType(), default="N"),
    Column("has_loan", StringType(), default="N"),
    Column("total_balance", DecimalType(18, 2)),
    Column("total_credit_limit", DecimalType(18, 2)),
    Column("credit_utilization_pct", DecimalType(5, 2)),
    Column("load_ts", TimestampType()),
], pi=("customer_id",))

STG_TXN_SUMMARY = _T("STG_TXN_SUMMARY", [
    Column("customer_id", LongType(), nullable=False),
    Column("account_id", LongType(), nullable=False),
    Column("account_type", StringType()),
    Column("summary_period_start", DateType()),
    Column("summary_period_end", DateType()),
    Column("txn_count_total", IntegerType()),
    Column("txn_count_debit", IntegerType()),
    Column("txn_count_credit", IntegerType()),
    Column("txn_count_fee", IntegerType()),
    Column("amt_total_debit", DecimalType(18, 2)),
    Column("amt_total_credit", DecimalType(18, 2)),
    Column("amt_total_fees", DecimalType(18, 2)),
    Column("amt_avg_debit", DecimalType(15, 2)),
    Column("amt_avg_credit", DecimalType(15, 2)),
    Column("amt_max_single_debit", DecimalType(15, 2)),
    Column("amt_max_single_credit", DecimalType(15, 2)),
    Column("distinct_merchants", IntegerType()),
    Column("top_merchant_category", StringType()),
    Column("pct_atm", DecimalType(5, 2)),
    Column("pct_pos", DecimalType(5, 2)),
    Column("pct_web", DecimalType(5, 2)),
    Column("pct_mobile", DecimalType(5, 2)),
    Column("days_since_last_txn", IntegerType()),
    Column("load_ts", TimestampType()),
], pi=("customer_id", "account_id"))

STG_RISK_FACTORS = _T("STG_RISK_FACTORS", [
    Column("customer_id", LongType(), nullable=False),
    Column("account_overdraft_cnt", IntegerType()),
    Column("nsf_fee_total", DecimalType(15, 2)),
    Column("large_withdrawal_cnt", IntegerType()),
    Column("large_withdrawal_amt", DecimalType(18, 2)),
    Column("avg_daily_balance_30d", DecimalType(15, 2)),
    Column("avg_daily_balance_90d", DecimalType(15, 2)),
    Column("balance_volatility", DecimalType(10, 4)),
    Column("credit_util_ratio", DecimalType(5, 4)),
    Column("payment_ontime_pct", DecimalType(5, 2)),
    Column("payment_late_cnt", IntegerType()),
    Column("months_since_last_late", IntegerType()),
    Column("external_credit_score", IntegerType()),
    Column("debit_velocity_7d", DecimalType(15, 2)),
    Column("debit_velocity_30d", DecimalType(15, 2)),
    Column("new_merchant_cnt_30d", IntegerType()),
    Column("international_txn_cnt", IntegerType()),
    Column("high_risk_merchant_cnt", IntegerType()),
    Column("load_ts", TimestampType()),
], pi=("customer_id",))

# --------------------------------------------------------------------------- #
# Data product tables (ddl/02_data_product_tables.sql)                         #
# --------------------------------------------------------------------------- #
CUSTOMER_SEGMENTS = _T("CUSTOMER_SEGMENTS", [
    Column("customer_id", LongType(), nullable=False),
    Column("segment_name", StringType()),
    Column("segment_id", ShortType()),
    Column("subsegment_id", ShortType()),
    Column("lifetime_value_score", DecimalType(10, 2)),
    Column("engagement_score", DecimalType(5, 2)),
    Column("digital_adoption_score", DecimalType(5, 2)),
    Column("product_breadth_index", DecimalType(5, 2)),
    Column("tenure_group", StringType()),
    Column("age_group", StringType()),
    Column("balance_tier", StringType()),
    Column("channel_preference", StringType()),
    Column("cross_sell_flag", StringType(), default="N"),
    Column("upsell_flag", StringType(), default="N"),
    Column("retention_risk_flag", StringType(), default="N"),
    Column("model_version", StringType()),
    Column("effective_date", DateType()),
    Column("load_ts", TimestampType()),
], pi=("customer_id",))

TRANSACTION_ANALYTICS = _T("TRANSACTION_ANALYTICS", [
    Column("customer_id", LongType(), nullable=False),
    Column("reporting_period", StringType()),
    Column("total_accounts", ShortType()),
    Column("active_accounts", ShortType()),
    Column("total_transactions", IntegerType()),
    Column("total_debit_amt", DecimalType(18, 2)),
    Column("total_credit_amt", DecimalType(18, 2)),
    Column("net_cash_flow", DecimalType(18, 2)),
    Column("avg_transaction_size", DecimalType(15, 2)),
    Column("monthly_spend_trend", StringType()),
    Column("spend_percentile", DecimalType(5, 2)),
    Column("top_spend_category", StringType()),
    Column("digital_txn_pct", DecimalType(5, 2)),
    Column("fee_income", DecimalType(15, 2)),
    Column("interest_income", DecimalType(15, 2)),
    Column("revenue_contribution", DecimalType(15, 2)),
    Column("anomaly_flag", StringType(), default="N"),
    Column("model_version", StringType()),
    Column("effective_date", DateType()),
    Column("load_ts", TimestampType()),
], pi=("customer_id",), part=("reporting_period",))

CUSTOMER_RISK_SCORES = _T("CUSTOMER_RISK_SCORES", [
    Column("customer_id", LongType(), nullable=False),
    Column("composite_risk_score", DecimalType(6, 2)),
    Column("risk_tier", StringType()),
    Column("probability_of_default", DecimalType(7, 6)),
    Column("credit_risk_component", DecimalType(5, 2)),
    Column("behaviour_risk_component", DecimalType(5, 2)),
    Column("velocity_risk_component", DecimalType(5, 2)),
    Column("bureau_score_component", DecimalType(5, 2)),
    Column("payment_history_component", DecimalType(5, 2)),
    Column("primary_risk_driver", StringType()),
    Column("secondary_risk_driver", StringType()),
    Column("score_delta_30d", DecimalType(6, 2)),
    Column("watch_list_flag", StringType(), default="N"),
    Column("review_required_flag", StringType(), default="N"),
    Column("model_version", StringType()),
    Column("effective_date", DateType()),
    Column("load_ts", TimestampType()),
], pi=("customer_id",))

CUSTOMER_MASTER_PROFILE = _T("CUSTOMER_MASTER_PROFILE", [
    Column("customer_id", LongType(), nullable=False),
    Column("full_name", StringType()),
    Column("age", ShortType()),
    Column("state_code", StringType()),
    Column("customer_since", DateType()),
    Column("tenure_months", IntegerType()),
    Column("customer_status", StringType()),
    Column("segment_name", StringType()),
    Column("lifetime_value_score", DecimalType(10, 2)),
    Column("engagement_score", DecimalType(5, 2)),
    Column("total_accounts", ShortType()),
    Column("active_accounts", ShortType()),
    Column("total_balance", DecimalType(18, 2)),
    Column("total_credit_limit", DecimalType(18, 2)),
    Column("credit_utilization_pct", DecimalType(5, 2)),
    Column("monthly_transactions", IntegerType()),
    Column("monthly_spend", DecimalType(18, 2)),
    Column("net_cash_flow", DecimalType(18, 2)),
    Column("top_spend_category", StringType()),
    Column("digital_txn_pct", DecimalType(5, 2)),
    Column("composite_risk_score", DecimalType(6, 2)),
    Column("risk_tier", StringType()),
    Column("probability_of_default", DecimalType(7, 6)),
    Column("watch_list_flag", StringType(), default="N"),
    Column("cross_sell_flag", StringType(), default="N"),
    Column("upsell_flag", StringType(), default="N"),
    Column("retention_risk_flag", StringType(), default="N"),
    Column("model_version", StringType()),
    Column("effective_date", DateType()),
    Column("load_ts", TimestampType()),
], pi=("customer_id",))

STAGING_TABLES = {t.name: t for t in (STG_CUSTOMER_360, STG_TXN_SUMMARY, STG_RISK_FACTORS)}
DATA_PRODUCT_TABLES = {
    t.name: t for t in (
        CUSTOMER_SEGMENTS, TRANSACTION_ANALYTICS,
        CUSTOMER_RISK_SCORES, CUSTOMER_MASTER_PROFILE,
    )
}
ALL_TABLES = {**SOURCE_TABLES, **STAGING_TABLES, **DATA_PRODUCT_TABLES}


def enforce_schema(df, spec: TableSpec):
    """Project ``df`` onto the exact DDL column order and types.

    Every output column is selected in DDL order and cast to its declared Spark
    type so the emitted DataFrame's schema matches the target table byte-for-byte
    (Rules R3 -- schema contracts).
    """
    from pyspark.sql import functions as F

    select_exprs = []
    for col in spec.columns:
        if col.name in df.columns:
            expr = F.col(col.name)
        elif col.default is not None:
            expr = F.lit(col.default)
        else:
            expr = F.lit(None)
        select_exprs.append(expr.cast(col.dtype).alias(col.name))
    return df.select(*select_exprs)


def column_order(spec_name: str) -> Sequence[str]:
    return ALL_TABLES[spec_name].column_names


def schema_signature(struct: StructType) -> list[tuple[str, str]]:
    """(name, type) pairs, ignoring nullability.

    Spark does not preserve NOT NULL flags through arbitrary transforms or a
    Parquet round-trip, so the column-name + type list is the meaningful,
    comparable contract.  NOT NULL is enforced separately via
    :func:`common.validation.validate_table`.
    """
    return [(f.name, f.dataType.simpleString()) for f in struct.fields]


def schema_matches(df, spec: TableSpec) -> bool:
    """True when ``df`` matches ``spec`` on column order + types."""
    return schema_signature(df.schema) == schema_signature(spec.struct)


def assert_schema(df, spec: TableSpec) -> None:
    actual = schema_signature(df.schema)
    expected = schema_signature(spec.struct)
    if actual != expected:
        diff = [
            (e, a) for e, a in zip(expected, actual) if e != a
        ] + [("<len>", f"{len(expected)} vs {len(actual)}")] * (len(expected) != len(actual))
        raise AssertionError(f"{spec.name} schema mismatch (expected, actual): {diff}")
