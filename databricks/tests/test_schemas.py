"""The gold contracts must stay column-for-column identical to the Teradata DDL."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
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

from shared import schemas

DDL_DIR = Path(__file__).resolve().parents[2] / "ddl"

TERADATA_TYPES = {
    "BIGINT": LongType(),
    "INTEGER": IntegerType(),
    "SMALLINT": ShortType(),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
    "CHAR": StringType(),
    "VARCHAR": StringType(),
}

_TABLE_RE = re.compile(
    r"CREATE\s+(?:MULTISET\s+|SET\s+)?TABLE\s+\w+\.(\w+)[^(]*\((.*?)\n\)", re.S
)
_COLUMN_RE = re.compile(r"^\s*(\w+)\s+([A-Z]+)(?:\(([\d,\s]+)\))?", re.M)


def parse_ddl(path: Path) -> dict[str, list[tuple[str, object]]]:
    """Extract ``{table: [(column, spark_type), ...]}`` from a Teradata DDL file."""
    sql = re.sub(r"--.*$", "", path.read_text(), flags=re.M)
    tables: dict[str, list[tuple[str, object]]] = {}
    for table, body in _TABLE_RE.findall(sql):
        columns: list[tuple[str, object]] = []
        for name, base_type, args in _COLUMN_RE.findall(body):
            if base_type not in TERADATA_TYPES and base_type != "DECIMAL":
                continue
            if base_type == "DECIMAL":
                precision, scale = (int(a) for a in args.split(","))
                columns.append((name, DecimalType(precision, scale)))
            else:
                columns.append((name, TERADATA_TYPES[base_type]))
        tables[table] = columns
    return tables


@pytest.mark.parametrize(
    ("ddl_file", "contracts"),
    [
        ("00_source_tables.sql", schemas.SOURCE_SCHEMAS),
        ("01_staging_tables.sql", schemas.SILVER_SCHEMAS),
        ("02_data_product_tables.sql", schemas.GOLD_SCHEMAS),
    ],
)
def test_contracts_match_teradata_ddl(ddl_file: str, contracts: dict[str, StructType]) -> None:
    parsed = parse_ddl(DDL_DIR / ddl_file)
    for table, columns in parsed.items():
        if table not in contracts:  # WRK_* scratch tables have no Delta equivalent
            continue
        expected = [(name, dtype) for name, dtype in columns]
        actual = [(f.name, f.dataType) for f in contracts[table].fields]
        assert actual == expected, f"{table} drifted from ddl/{ddl_file}"


def test_every_gold_product_is_covered() -> None:
    assert set(schemas.GOLD_SCHEMAS) == {
        "CUSTOMER_SEGMENTS",
        "TRANSACTION_ANALYTICS",
        "CUSTOMER_RISK_SCORES",
        "CUSTOMER_MASTER_PROFILE",
    }


def test_conform_reorders_renames_and_casts(spark) -> None:
    schema = StructType([
        StructField("CUSTOMER_ID", LongType(), False),
        StructField("SEGMENT_NAME", StringType()),
        StructField("ENGAGEMENT_SCORE", DecimalType(5, 2)),
    ])
    df = spark.createDataFrame(
        [("premium", 12, 99.126)], "segment_name string, customer_id int, engagement_score double"
    )

    conformed = schemas.conform(df, schema)

    assert conformed.schema.fieldNames() == ["CUSTOMER_ID", "SEGMENT_NAME", "ENGAGEMENT_SCORE"]
    assert [f.dataType for f in conformed.schema.fields] == [
        LongType(),
        StringType(),
        DecimalType(5, 2),
    ]
    row = conformed.collect()[0]
    assert row.CUSTOMER_ID == 12
    assert float(row.ENGAGEMENT_SCORE) == 99.13


def test_conform_rejects_a_missing_contract_column(spark) -> None:
    schema = StructType([StructField("CUSTOMER_ID", LongType()), StructField("AGE", ShortType())])
    df = spark.createDataFrame([(1,)], "CUSTOMER_ID long")
    with pytest.raises(ValueError, match="AGE"):
        schemas.conform(df, schema)


def test_schema_diff_reports_missing_extra_type_and_order() -> None:
    expected = StructType([
        StructField("A", LongType()),
        StructField("B", StringType()),
        StructField("C", DecimalType(5, 2)),
    ])
    actual = StructType([
        StructField("B", StringType()),
        StructField("A", StringType()),
        StructField("D", StringType()),
    ])

    diffs = schemas.schema_diff(actual, expected)

    assert any("missing column C" in d for d in diffs)
    assert any("unexpected column D" in d for d in diffs)
    assert any(d.startswith("A: expected bigint") for d in diffs)
    assert any(d.startswith("column order") for d in diffs)
    assert schemas.schema_diff(expected, expected) == []


def test_every_managed_table_has_a_clustering_key() -> None:
    """Delta clustering replaces the Teradata PRIMARY INDEX on every table."""
    for table in {**schemas.SILVER_SCHEMAS, **schemas.GOLD_SCHEMAS}:
        assert schemas.CLUSTER_KEYS[table][0] == "CUSTOMER_ID"
