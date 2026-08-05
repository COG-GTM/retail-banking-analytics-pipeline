"""The DDL contract is enforced programmatically, not by inspection."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from common import schemas
from common.schemas import (
    ALL_SPECS,
    SchemaMismatchError,
    assert_schema,
    enforce_schema,
    spec_for,
)

pytestmark = pytest.mark.nonfunctional

DDL_FILES = (
    "ddl/00_source_tables.sql",
    "ddl/01_staging_tables.sql",
    "ddl/02_data_product_tables.sql",
)

TYPE_PATTERN = re.compile(
    r"^\s{4}([A-Z_0-9]+)\s+(BIGINT|INTEGER|SMALLINT|BYTEINT|DATE|TIMESTAMP\(\d\)|"
    r"VARCHAR\(\d+\)|CHAR\(\d+\)|DECIMAL\(\d+,\s*\d+\))",
    re.IGNORECASE,
)


def _ddl_tables(repo_root: Path) -> dict[str, list[str]]:
    """Columns per table, in declaration order, straight out of the DDL text."""

    tables: dict[str, list[str]] = {}
    current: str | None = None
    for relative in DDL_FILES:
        for line in (repo_root / relative).read_text(encoding="utf-8").splitlines():
            create = re.match(
                r"CREATE\s+(?:MULTISET\s+|SET\s+)?TABLE\s+([A-Z_]+)\.([A-Z_0-9]+)", line, re.I
            )
            if create:
                current = f"{create.group(1).upper()}.{create.group(2).upper()}"
                tables[current] = []
                continue
            if current is None:
                continue
            if line.startswith(")") or line.strip().startswith("PRIMARY INDEX"):
                current = None
                continue
            match = TYPE_PATTERN.match(line)
            if match:
                tables[current].append(match.group(1).upper())
    return tables


def test_every_ddl_table_has_a_spec_with_the_same_columns_in_the_same_order(repo_root):
    ddl_tables = _ddl_tables(repo_root)
    assert ddl_tables, "no CREATE TABLE statements were parsed out of the DDL"

    for qualified_name, columns in ddl_tables.items():
        spec = schemas.SPECS_BY_QUALIFIED_NAME.get(qualified_name)
        assert spec is not None, f"{qualified_name} has no TableSpec"
        assert list(spec.column_names) == columns, qualified_name


def test_only_the_documented_tables_are_inferred():
    inferred = {spec.qualified_name for spec in ALL_SPECS if spec.inferred}
    assert inferred == {
        "CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES",
        "ETL_STAGING_DB.ETL_RUN_LOG",
        "ETL_STAGING_DB.PIPELINE_AUDIT",
    }


def test_partitioning_and_primary_index_follow_the_ddl():
    assert schemas.TRANSACTION_ANALYTICS.partition_by == ("REPORTING_PERIOD",)
    assert schemas.TRANSACTIONS.partition_by == ("TRANSACTION_DATE",)
    assert schemas.CUSTOMER_MASTER_PROFILE.primary_index == ("CUSTOMER_ID",)
    assert schemas.STG_TXN_SUMMARY.primary_index == ("CUSTOMER_ID", "ACCOUNT_ID")


def test_defaults_from_the_ddl_are_applied(spark):
    df = spark.createDataFrame(
        [(1, None)],
        StructType(
            [
                StructField("CUSTOMER_ID", schemas.LongType()),
                StructField("HAS_CHECKING", StringType()),
            ]
        ),
    )

    enforced = enforce_schema(df, schemas.STG_CUSTOMER_360, allow_missing=True)
    row = enforced.collect()[0]

    assert row["HAS_CHECKING"] == "N"
    assert row["HAS_LOAN"] == "N"
    assert row["CITY"] is None
    assert_schema(enforced, schemas.STG_CUSTOMER_360)


def test_integer_columns_survive_float_formatted_csv_values(spark):
    df = spark.createDataFrame([("1", "3.0")], "CUSTOMER_ID string, NUM_ACCOUNTS string")

    row = enforce_schema(df, schemas.STG_CUSTOMER_360, allow_missing=True).collect()[0]

    assert (row["CUSTOMER_ID"], row["NUM_ACCOUNTS"]) == (1, 3)


def test_missing_column_without_a_default_is_an_error(spark):
    df = spark.createDataFrame([(1,)], "CUSTOMER_ID bigint")

    with pytest.raises(SchemaMismatchError, match="is missing"):
        enforce_schema(df, schemas.STG_CUSTOMER_360)


def test_assert_schema_detects_reordering_and_wrong_types(spark, make_df):
    good = make_df(schemas.CUSTOMER_RISK_SCORES, [])
    assert_schema(good, schemas.CUSTOMER_RISK_SCORES)

    reordered = good.select("RISK_TIER", *[c for c in good.columns if c != "RISK_TIER"])
    with pytest.raises(SchemaMismatchError, match="column mismatch"):
        assert_schema(reordered, schemas.CUSTOMER_RISK_SCORES)

    retyped = good.withColumn("COMPOSITE_RISK_SCORE", F.col("COMPOSITE_RISK_SCORE").cast("double"))
    with pytest.raises(SchemaMismatchError, match="type mismatch"):
        assert_schema(retyped, schemas.CUSTOMER_RISK_SCORES)


def test_spec_lookup_is_case_insensitive_and_reports_unknown_tables():
    assert spec_for("core_banking_db", "customers") is schemas.CUSTOMERS
    with pytest.raises(KeyError, match="no DDL contract"):
        spec_for("CORE_BANKING_DB", "NOT_A_TABLE")


def test_decimal_precision_matches_the_ddl():
    assert schemas.CUSTOMER_RISK_SCORES.column("PROBABILITY_OF_DEFAULT").dtype == DecimalType(7, 6)
    assert schemas.STG_RISK_FACTORS.column("BALANCE_VOLATILITY").dtype == DecimalType(10, 4)
    assert schemas.STG_CUSTOMER_360.column("TOTAL_BALANCE").dtype == DecimalType(18, 2)
