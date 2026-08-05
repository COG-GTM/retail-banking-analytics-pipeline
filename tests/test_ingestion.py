"""Unit tests for STEP 1 / STEP 2 (``risk_scoring.ingestion``).

Fixtures are small in-memory DataFrames — the pure transforms never touch the
committed CSVs.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from risk_scoring import schemas
from risk_scoring.connections import Connections, DataBackend
from risk_scoring.config import PipelineConfig
from risk_scoring.ingestion import build_risk_features, read_risk_raw

RISK_RAW_SCHEMA = StructType([
    StructField("CUSTOMER_ID", LongType(), False),
    StructField("EXTERNAL_CREDIT_SCORE", IntegerType(), True),
    StructField("AVG_DAILY_BALANCE_30D", DoubleType(), True),
    StructField("AVG_DAILY_BALANCE_90D", DoubleType(), True),
    StructField("DEBIT_VELOCITY_7D", DoubleType(), True),
    StructField("DEBIT_VELOCITY_30D", DoubleType(), True),
    StructField("PAYMENT_LATE_CNT", IntegerType(), True),
    StructField("CUSTOMER_STATUS", StringType(), True),
])


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test_ingestion")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class _DictBackend(DataBackend):
    """In-memory backend: ``{(database, table): DataFrame}``."""

    def __init__(self, tables: dict[tuple[str, str], "SparkSession"]):
        self.tables = tables
        self.reads: list[tuple[str, str]] = []

    def read_table(self, database, table, schema=None):
        self.reads.append((database, table))
        df = self.tables[(database, table)]
        if schema is None:
            return df
        from pyspark.sql import functions as F

        return df.select(*[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in schema.fields
            if f.name in df.columns
        ])

    def overwrite_table(self, df, database, table):  # pragma: no cover - unused
        raise NotImplementedError


def _risk_raw(spark, rows):
    """Build a RISK_RAW-shaped DataFrame from tuples matching RISK_RAW_SCHEMA."""
    return spark.createDataFrame(rows, RISK_RAW_SCHEMA)


def _features(spark, rows):
    return {
        row["CUSTOMER_ID"]: row.asDict()
        for row in build_risk_features(_risk_raw(spark, rows)).collect()
    }


# --------------------------------------------------------------------------- #
# STEP 1 — read_risk_raw                                                       #
# --------------------------------------------------------------------------- #


def _staging_connections(spark, risk_rows, customer_rows):
    config = PipelineConfig()
    risk_factors = spark.createDataFrame(
        risk_rows,
        StructType([
            StructField("CUSTOMER_ID", LongType(), False),
            StructField("EXTERNAL_CREDIT_SCORE", IntegerType(), True),
            StructField("PAYMENT_LATE_CNT", IntegerType(), True),
        ]),
    )
    customer_360 = spark.createDataFrame(
        customer_rows,
        StructType([
            StructField("CUSTOMER_ID", LongType(), False),
            StructField("TENURE_MONTHS", IntegerType(), True),
            StructField("NUM_ACTIVE_ACCOUNTS", IntegerType(), True),
            StructField("TOTAL_BALANCE", DoubleType(), True),
            StructField("CUSTOMER_STATUS", StringType(), True),
            # An extra column that must not leak into RISK_RAW.
            StructField("SEGMENT_CODE", StringType(), True),
        ]),
    )
    staging = config.databases.staging
    backend = _DictBackend({
        (staging, schemas.STG_RISK_FACTORS): risk_factors,
        (staging, schemas.STG_CUSTOMER_360): customer_360,
    })
    return Connections(spark, config, backend=backend), backend


def test_read_risk_raw_inner_join_and_status_filter(spark):
    connections, backend = _staging_connections(
        spark,
        risk_rows=[
            (1, 700, 0),  # active     -> kept
            (2, 700, 0),  # inactive   -> dropped by the CUSTOMER_STATUS filter
            (3, 700, 0),  # no STG_CUSTOMER_360 row -> dropped by the inner join
        ],
        customer_rows=[
            (1, 24, 2, 100.0, "A", "MASS"),
            (2, 36, 1, 50.0, "I", "MASS"),
            (4, 12, 1, 10.0, "A", "MASS"),  # no risk-factor row -> dropped
        ],
    )

    risk_raw = read_risk_raw(connections)
    rows = {row["CUSTOMER_ID"]: row for row in risk_raw.collect()}

    assert set(rows) == {1}
    assert rows[1]["CUSTOMER_STATUS"] == "A"
    assert rows[1]["TENURE_MONTHS"] == 24
    assert rows[1]["NUM_ACTIVE_ACCOUNTS"] == 2
    assert rows[1]["TOTAL_BALANCE"] == 100.0
    # Only the four contract columns come across from STG_CUSTOMER_360.
    assert "SEGMENT_CODE" not in risk_raw.columns
    # Exactly one CUSTOMER_ID, no ambiguous duplicate from the join.
    assert risk_raw.columns.count("CUSTOMER_ID") == 1
    assert risk_raw.select("CUSTOMER_ID").columns == ["CUSTOMER_ID"]
    # Both tables are read through the staging database of the config.
    assert backend.reads == [
        (connections.config.databases.staging, schemas.STG_RISK_FACTORS),
        (connections.config.databases.staging, schemas.STG_CUSTOMER_360),
    ]


def test_read_risk_raw_column_order_is_risk_factors_then_customer_360(spark):
    connections, _ = _staging_connections(
        spark,
        risk_rows=[(1, 700, 0)],
        customer_rows=[(1, 24, 2, 100.0, "A", "MASS")],
    )

    # STG_RISK_FACTORS columns first, in STG_RISK_FACTORS_SCHEMA order (the
    # backend projects through the schema), then the STG_CUSTOMER_360 columns.
    assert read_risk_raw(connections).columns == [
        "CUSTOMER_ID",
        "PAYMENT_LATE_CNT",
        "EXTERNAL_CREDIT_SCORE",
        "TENURE_MONTHS",
        "NUM_ACTIVE_ACCOUNTS",
        "TOTAL_BALANCE",
        "CUSTOMER_STATUS",
    ]


def test_read_risk_raw_applies_the_analytic_double_schema(spark):
    config = PipelineConfig()
    staging = config.databases.staging
    risk_factors = spark.createDataFrame(
        [(1, "12.50", "1000.00")],
        StructType([
            StructField("CUSTOMER_ID", LongType(), False),
            StructField("NSF_FEE_TOTAL", StringType(), True),
            StructField("AVG_DAILY_BALANCE_30D", StringType(), True),
        ]),
    )
    customer_360 = spark.createDataFrame(
        [(1, 24, 2, 100.0, "A")],
        StructType([
            StructField("CUSTOMER_ID", LongType(), False),
            StructField("TENURE_MONTHS", IntegerType(), True),
            StructField("NUM_ACTIVE_ACCOUNTS", IntegerType(), True),
            StructField("TOTAL_BALANCE", DoubleType(), True),
            StructField("CUSTOMER_STATUS", StringType(), True),
        ]),
    )
    connections = Connections(
        spark,
        config,
        backend=_DictBackend({
            (staging, schemas.STG_RISK_FACTORS): risk_factors,
            (staging, schemas.STG_CUSTOMER_360): customer_360,
        }),
    )

    dtypes = dict(read_risk_raw(connections).dtypes)
    assert dtypes["NSF_FEE_TOTAL"] == "double"
    assert dtypes["AVG_DAILY_BALANCE_30D"] == "double"


# --------------------------------------------------------------------------- #
# STEP 2 — build_risk_features                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("score", "expected_score", "expected_norm"),
    [
        (720, 720, (720 - 300) / 550 * 100),  # normal value, kept
        (0, 680, (680 - 300) / 550 * 100),  # BTEQ COALESCE of a missed join
        (-5, 680, (680 - 300) / 550 * 100),  # negative, <= 0 branch
        (None, 680, (680 - 300) / 550 * 100),  # SAS missing (.)
        (300, 300, 0.0),  # lower bound of the normalisation
        (850, 850, 100.0),  # upper bound of the normalisation
    ],
)
def test_bureau_score_imputation_and_normalisation(
    spark, score, expected_score, expected_norm
):
    features = _features(spark, [(1, score, 1.0, 1.0, 1.0, 1.0, 0, "A")])[1]

    assert features["EXTERNAL_CREDIT_SCORE"] == expected_score
    assert features["BUREAU_SCORE_NORM"] == pytest.approx(expected_norm)


def test_bureau_score_norm_reference_values(spark):
    features = _features(
        spark,
        [
            (1, 720, 1.0, 1.0, 1.0, 1.0, 0, "A"),
            (2, None, 1.0, 1.0, 1.0, 1.0, 0, "A"),
        ],
    )

    assert features[1]["BUREAU_SCORE_NORM"] == pytest.approx(76.363636, abs=1e-6)
    assert features[2]["BUREAU_SCORE_NORM"] == pytest.approx(69.090909, abs=1e-6)


@pytest.mark.parametrize(
    ("balance_30d", "balance_90d", "expected"),
    [
        (500.0, 1000.0, 0.5),  # positive denominator
        (500.0, 0.0, 1.0),  # zero denominator -> else branch
        (500.0, -1000.0, 1.0),  # negative denominator -> else branch
        (500.0, None, 1.0),  # missing denominator -> else branch (as in SAS)
        (None, 1000.0, None),  # missing numerator propagates, as SAS missing
    ],
)
def test_balance_trend_ratio(spark, balance_30d, balance_90d, expected):
    features = _features(
        spark, [(1, 700, balance_30d, balance_90d, 1.0, 1.0, 0, "A")]
    )[1]

    if expected is None:
        assert features["BALANCE_TREND_RATIO"] is None
    else:
        assert features["BALANCE_TREND_RATIO"] == pytest.approx(expected)


@pytest.mark.parametrize(
    ("velocity_7d", "velocity_30d", "expected"),
    [
        (100.0, 300.0, 100.0 * (30 / 7) / 300.0),  # 1.428571...
        (70.0, 300.0, 1.0),  # exactly flat velocity
        (100.0, 0.0, 1.0),  # zero denominator -> else branch
        (100.0, -300.0, 1.0),  # negative denominator -> else branch
        (100.0, None, 1.0),  # missing denominator -> else branch
        (None, 300.0, None),  # missing numerator propagates
    ],
)
def test_velocity_ratio(spark, velocity_7d, velocity_30d, expected):
    features = _features(
        spark, [(1, 700, 1.0, 1.0, velocity_7d, velocity_30d, 0, "A")]
    )[1]

    if expected is None:
        assert features["VELOCITY_RATIO"] is None
    else:
        assert features["VELOCITY_RATIO"] == pytest.approx(expected)


def test_velocity_ratio_annualisation_factor(spark):
    features = _features(spark, [(1, 700, 1.0, 1.0, 100.0, 300.0, 0, "A")])[1]

    assert features["VELOCITY_RATIO"] == pytest.approx(1.428571, abs=1e-6)


@pytest.mark.parametrize(
    ("late_cnt", "expected"),
    [(0, 0), (2, 0), (3, 1), (None, 0)],
)
def test_default_flag(spark, late_cnt, expected):
    features = _features(spark, [(1, 700, 1.0, 1.0, 1.0, 1.0, late_cnt, "A")])[1]

    assert features["DEFAULT_FLAG"] == expected


def test_derived_column_types_and_input_preservation(spark):
    risk_raw = _risk_raw(spark, [(1, 700, 500.0, 1000.0, 100.0, 300.0, 3, "A")])

    features = build_risk_features(risk_raw)
    dtypes = dict(features.dtypes)

    # No input column dropped or reordered; the four derived columns are appended.
    assert features.columns == risk_raw.columns + list(schemas.RISK_FEATURE_COLUMNS)
    assert dtypes["BUREAU_SCORE_NORM"] == "double"
    assert dtypes["BALANCE_TREND_RATIO"] == "double"
    assert dtypes["VELOCITY_RATIO"] == "double"
    # SAS boolean-as-numeric: 0/1, not a Spark boolean.
    assert dtypes["DEFAULT_FLAG"] == "int"
    # EXTERNAL_CREDIT_SCORE is re-imputed in place, keeping its input type.
    assert dtypes["EXTERNAL_CREDIT_SCORE"] == "int"


def test_build_risk_features_is_deterministic(spark):
    rows = [
        (1, 0, 500.0, 1000.0, 100.0, 300.0, 3, "A"),
        (2, 720, None, 0.0, 10.0, None, 1, "A"),
    ]

    first = build_risk_features(_risk_raw(spark, rows)).collect()
    second = build_risk_features(_risk_raw(spark, rows)).collect()

    assert first == second
