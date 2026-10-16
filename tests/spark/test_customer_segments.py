"""Parity tests: synapse/spark/jobs/customer_segments.py vs the committed SAS output.

Input:     data/02_bteq_staging/stg_customer_360.csv
Reference: data/03_sas_data_products/customer_segments.csv (407 rows, as-of 2026-04-10)

The reference was produced by scikit-learn KMeans(n_init=10, random_state=42), not
real SAS, so cluster ids are arbitrary and segment_name is compared by agreement.
Rationale for SEGMENT_AGREEMENT_MIN: the same scikit-learn generator re-run with
random_state 0..19 agrees with the reference only 38-100% (median 77%); every run
whose SSE is within 0.02% of the reference SSE agrees >= 98.5%. A port that finds the
same optimum should therefore clear 95%; a port stuck in another local optimum
(Spark single-seed k-means: 43%) fails.

Known reference defect: the reference buckets AGE and TENURE_MONTHS with `<=` where the
SAS DATA step uses `<` (e.g. AGE 25 -> GEN_Z, TENURE_MONTHS 12 -> NEW). The port follows
SAS; test_reference_boundary_defect pins down that these boundary rows are the only diffs.
"""

from __future__ import annotations

import datetime
import math
import os
import sys
import types
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
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

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from synapse.spark.jobs import customer_segments as cs

AS_OF_DATE = datetime.date(2026, 4, 10)
EXPECTED_ROWS = 407
SEGMENT_AGREEMENT_MIN = 0.95
SEGMENT_SHARE_TOL = 0.03
NUMERIC_TOL = 0.01

STG_CUSTOMER_360_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType(), False),
        StructField("FIRST_NAME", StringType()),
        StructField("LAST_NAME", StringType()),
        StructField("DATE_OF_BIRTH", DateType()),
        StructField("AGE", ShortType()),
        StructField("CUSTOMER_SINCE", DateType()),
        StructField("TENURE_MONTHS", IntegerType()),
        StructField("CUSTOMER_STATUS", StringType()),
        StructField("SEGMENT_CODE", StringType()),
        StructField("BRANCH_ID", IntegerType()),
        StructField("PRIMARY_ADDRESS", StringType()),
        StructField("CITY", StringType()),
        StructField("STATE_CODE", StringType()),
        StructField("ZIP_CODE", StringType()),
        StructField("NUM_ACCOUNTS", ShortType()),
        StructField("NUM_ACTIVE_ACCOUNTS", ShortType()),
        StructField("HAS_CHECKING", StringType()),
        StructField("HAS_SAVINGS", StringType()),
        StructField("HAS_CREDIT", StringType()),
        StructField("HAS_LOAN", StringType()),
        StructField("TOTAL_BALANCE", DecimalType(18, 2)),
        StructField("TOTAL_CREDIT_LIMIT", DecimalType(18, 2)),
        StructField("CREDIT_UTILIZATION_PCT", DecimalType(5, 2)),
        StructField("LOAD_TS", TimestampType()),
    ]
)

# Reference uses `<=` on these SAS `<` cut points (see module docstring).
AGE_CUTS = [25, 41, 57, 76]
TENURE_CUTS = [12, 36, 84]
BOUNDARY_COLS = ["TENURE_GROUP", "AGE_GROUP", "UPSELL_FLAG"]

DETERMINISTIC_EXACT = [
    "SUBSEGMENT_ID",
    "BALANCE_TIER",
    "CHANNEL_PREFERENCE",
    "CROSS_SELL_FLAG",
    "RETENTION_RISK_FLAG",
    "MODEL_VERSION",
    "EFFECTIVE_DATE",
]
DETERMINISTIC_NUMERIC = [
    "LIFETIME_VALUE_SCORE",
    "ENGAGEMENT_SCORE",
    "DIGITAL_ADOPTION_SCORE",
    "PRODUCT_BREADTH_INDEX",
]


def _read_csv(spark, path: Path, schema: StructType):
    """Read every column as string (the CSVs hold e.g. '3.0' for SMALLINT), then cast to the DDL type."""
    raw = (
        spark.read.option("header", True)
        .schema(StructType([StructField(f.name, StringType()) for f in schema.fields]))
        .csv(str(path))
    )
    return raw.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields]
    )


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_customer_segments")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def stg_customer_360(spark):
    return _read_csv(
        spark,
        REPO_ROOT / "data/02_bteq_staging/stg_customer_360.csv",
        STG_CUSTOMER_360_SCHEMA,
    )


@pytest.fixture(scope="module")
def staging(stg_customer_360):
    return {
        r["CUSTOMER_ID"]: r.asDict()
        for r in stg_customer_360.where("CUSTOMER_STATUS = 'A'").collect()
    }


@pytest.fixture(scope="module")
def result_df(stg_customer_360):
    return cs.transform(stg_customer_360, AS_OF_DATE).cache()


@pytest.fixture(scope="module")
def actual(result_df):
    return {r["CUSTOMER_ID"]: r.asDict() for r in result_df.collect()}


@pytest.fixture(scope="module")
def expected(spark):
    rows = _read_csv(
        spark,
        REPO_ROOT / "data/03_sas_data_products/customer_segments.csv",
        cs.OUTPUT_SCHEMA,
    ).collect()
    return {r["CUSTOMER_ID"]: r.asDict() for r in rows}


def test_output_schema_matches_ddl(result_df):
    assert [(f.name, f.dataType) for f in result_df.schema.fields] == [
        (f.name, f.dataType) for f in cs.OUTPUT_SCHEMA.fields
    ]


def test_row_count_and_customer_ids(actual, expected):
    assert len(actual) == EXPECTED_ROWS
    assert len(expected) == EXPECTED_ROWS
    assert set(actual) == set(expected)


@pytest.mark.parametrize("col", DETERMINISTIC_EXACT)
def test_deterministic_columns_exact(actual, expected, col):
    mismatches = {
        cid: (actual[cid][col], expected[cid][col])
        for cid in expected
        if actual[cid][col] != expected[cid][col]
    }
    assert not mismatches, (
        f"{len(mismatches)} mismatches in {col}, e.g. {list(mismatches.items())[:5]}"
    )


def _bucket_le(value, cuts, labels):
    return next((label for cut, label in zip(cuts, labels) if value <= cut), labels[-1])


def test_reference_boundary_defect(actual, expected, staging):
    """Port matches the reference everywhere except rows sitting exactly on a SAS cut point."""
    on_boundary = {
        cid
        for cid, s in staging.items()
        if s["AGE"] in AGE_CUTS or s["TENURE_MONTHS"] in TENURE_CUTS
    }
    diffs = Counter()
    for col in BOUNDARY_COLS:
        bad = {cid for cid in expected if actual[cid][col] != expected[cid][col]}
        assert bad <= on_boundary, (
            f"{col}: unexplained mismatches {sorted(bad - on_boundary)[:10]}"
        )
        diffs[col] = len(bad)
    print(
        f"\nboundary-rule diffs vs reference (SAS '<' vs reference '<='): {dict(diffs)}"
    )

    for cid, s in staging.items():
        e = expected[cid]
        tenure_le = _bucket_le(
            s["TENURE_MONTHS"],
            TENURE_CUTS,
            ["NEW (<1yr)", "DEVELOPING (1-3yr)", "ESTABLISHED (3-7yr)", "LOYAL (7yr+)"],
        )
        age_le = _bucket_le(
            s["AGE"], AGE_CUTS, ["GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER", "SILENT"]
        )
        assert (tenure_le, age_le) == (e["TENURE_GROUP"], e["AGE_GROUP"]), cid
        upsell_le = (
            "Y"
            if actual[cid]["BALANCE_TIER"] == "MODERATE" and tenure_le != "NEW (<1yr)"
            else "N"
        )
        assert upsell_le == e["UPSELL_FLAG"], cid


@pytest.mark.parametrize("col", DETERMINISTIC_NUMERIC)
def test_deterministic_numeric_within_tolerance(actual, expected, col):
    def diff(cid):
        a, e = actual[cid][col], expected[cid][col]
        if a is None or e is None:
            return 0.0 if a is e else math.inf
        return abs(float(a) - float(e))

    worst = max(expected, key=diff)
    assert diff(worst) <= NUMERIC_TOL, (
        f"{col}: customer {worst} differs by {diff(worst)}"
    )


def test_constants(actual):
    rows = list(actual.values())
    assert {r["MODEL_VERSION"] for r in rows} == {"SEG_V3.2"}
    assert {r["EFFECTIVE_DATE"] for r in rows} == {AS_OF_DATE}
    assert {r["DIGITAL_ADOPTION_SCORE"] for r in rows} == {Decimal("0.00")}
    assert all(r["LOAD_TS"] is not None for r in rows)


def test_segment_ids_map_one_to_one_to_names(actual):
    pairs = {(r["SEGMENT_ID"], r["SEGMENT_NAME"]) for r in actual.values()}
    assert {sid for sid, _ in pairs} == set(range(1, cs.N_CLUSTERS + 1))
    assert {name for _, name in pairs} == set(cs.SEGMENT_LABELS)
    assert len(pairs) == cs.N_CLUSTERS


def test_segment_name_agreement(actual, expected):
    agree = sum(
        actual[cid]["SEGMENT_NAME"] == expected[cid]["SEGMENT_NAME"] for cid in expected
    ) / len(expected)
    print(f"\nsegment_name agreement: {agree:.2%} ({len(expected)} customers)")
    assert agree >= SEGMENT_AGREEMENT_MIN


def test_segment_size_distribution(actual, expected):
    n = len(expected)
    act = Counter(r["SEGMENT_NAME"] for r in actual.values())
    exp = Counter(r["SEGMENT_NAME"] for r in expected.values())
    for name in cs.SEGMENT_LABELS:
        print(f"{name:<20} actual={act[name]:>4} expected={exp[name]:>4}")
        assert abs(act[name] - exp[name]) / n <= SEGMENT_SHARE_TOL, name


def test_sas_missing_value_semantics(spark):
    """SAS treats a missing numeric as smaller than any number in `<` comparisons."""
    row = {f.name: None for f in STG_CUSTOMER_360_SCHEMA.fields}
    rows = []
    for i in range(1, 11):
        r = dict(
            row,
            CUSTOMER_ID=i,
            CUSTOMER_STATUS="A",
            AGE=30 + i,
            TENURE_MONTHS=12 * i,
            NUM_ACCOUNTS=2,
            NUM_ACTIVE_ACCOUNTS=2,
            HAS_CHECKING="Y",
            TOTAL_BALANCE=Decimal(1000 * i),
            CREDIT_UTILIZATION_PCT=Decimal("10.00"),
        )
        rows.append(r)
    rows.append(dict(row, CUSTOMER_ID=99, CUSTOMER_STATUS="A"))
    rows.append(dict(row, CUSTOMER_ID=100, CUSTOMER_STATUS="I", AGE=40))
    out = {
        r["CUSTOMER_ID"]: r
        for r in cs.transform(
            spark.createDataFrame(rows, STG_CUSTOMER_360_SCHEMA), AS_OF_DATE, n_init=2
        ).collect()
    }
    assert 100 not in out
    m = out[99]
    assert (m["TENURE_GROUP"], m["AGE_GROUP"], m["BALANCE_TIER"]) == (
        "NEW (<1yr)",
        "GEN_Z",
        "LOW",
    )
    assert m["PRODUCT_BREADTH_INDEX"] == Decimal("0.00")
    assert m["LIFETIME_VALUE_SCORE"] is None and m["ENGAGEMENT_SCORE"] is None
    assert (m["CROSS_SELL_FLAG"], m["UPSELL_FLAG"], m["RETENTION_RISK_FLAG"]) == (
        "N",
        "N",
        "N",
    )
    assert m["SEGMENT_NAME"] in cs.SEGMENT_LABELS


def test_stdize_uses_observed_values_only(spark):
    """PROC STDIZE: mean/std from non-missing values; missing -> 0 after standardisation."""
    rows = [(i, 0.0 if i == 1 else (1.0 if i == 2 else None)) for i in (1, 2, 3)]
    df = spark.createDataFrame(rows, "CUSTOMER_ID long, ACCT_RATIO double")
    for c in cs.CLUSTER_FEATURES:
        if c != "ACCT_RATIO":
            df = df.withColumn(c, F.lit(1.0))
    z = {
        r["CUSTOMER_ID"]: r["_features"][cs.CLUSTER_FEATURES.index("ACCT_RATIO")]
        for r in cs._standardise(df).collect()
    }
    assert math.isclose(z[1], -math.sqrt(0.5)) and math.isclose(z[2], math.sqrt(0.5))
    assert z[3] == 0.0


def test_transform_leaves_no_cached_data(spark, stg_customer_360):
    before = spark.sparkContext._jsc.getPersistentRDDs().size()
    cs.transform(stg_customer_360, AS_OF_DATE, n_init=2).collect()
    assert spark.sparkContext._jsc.getPersistentRDDs().size() == before


class _FakePipelineUtils(types.ModuleType):
    class ValidationError(Exception):
        pass

    def __init__(self, source_df, fail_validation=False):
        super().__init__("synapse.spark.common.pipeline_utils")
        self.source_df = source_df
        self.fail_validation = fail_validation
        self.calls = []

    def get_run_params(self, argv=None):
        self.calls.append(("get_run_params", argv))
        return types.SimpleNamespace(
            as_of_date=AS_OF_DATE,
            env="dev",
            lookback_months=12,
            risk_score_threshold=700,
        )

    def read_table(self, spark, schema, table):
        self.calls.append(("read_table", schema, table))
        return self.source_df

    def validate_table(self, df, key_cols, not_null_cols, min_rows=1):
        self.calls.append(("validate_table", key_cols, not_null_cols, min_rows))
        if self.fail_validation:
            raise self.ValidationError("boom")

    def write_table(self, df, schema, table, mode="overwrite", pre_delete_where=None):
        self.calls.append(("write_table", schema, table, mode, df.count()))

    def log_step(self, spark, job_name, step_name, status, row_count=None, msg=None):
        self.calls.append(("log_step", job_name, step_name, status, row_count))


def _install_fake(monkeypatch, fake):
    parent = types.ModuleType("synapse.spark.common")
    parent.pipeline_utils = fake
    monkeypatch.setitem(sys.modules, "synapse.spark.common", parent)
    monkeypatch.setitem(sys.modules, "synapse.spark.common.pipeline_utils", fake)


def test_main_reads_validates_writes_and_logs(monkeypatch, stg_customer_360, result_df):
    fake = _FakePipelineUtils(stg_customer_360)
    _install_fake(monkeypatch, fake)
    seen = {}

    def fake_transform(df, as_of_date, **params):
        seen.update(df=df, as_of_date=as_of_date)
        return result_df

    monkeypatch.setattr(cs, "transform", fake_transform)
    assert cs.main(["--as-of-date", "2026-04-10", "--env", "dev"]) == 0
    assert seen == {"df": stg_customer_360, "as_of_date": AS_OF_DATE}
    names = [c[0] for c in fake.calls]
    assert names == [
        "get_run_params",
        "log_step",
        "read_table",
        "validate_table",
        "write_table",
        "log_step",
    ]
    assert ("read_table", "stg", "STG_CUSTOMER_360") in fake.calls
    assert (
        "validate_table",
        ["CUSTOMER_ID"],
        ["CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"],
        1,
    ) in fake.calls
    assert (
        "write_table",
        "dp",
        "CUSTOMER_SEGMENTS",
        "overwrite",
        EXPECTED_ROWS,
    ) in fake.calls
    assert fake.calls[-1] == (
        "log_step",
        "01_sas_customer_segments",
        "01_CUSTOMER_SEG",
        "SUCCESS",
        EXPECTED_ROWS,
    )


def test_main_validation_failure_skips_write(monkeypatch, stg_customer_360, result_df):
    fake = _FakePipelineUtils(stg_customer_360, fail_validation=True)
    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(cs, "transform", lambda df, as_of_date, **params: result_df)
    assert cs.main([]) == 1
    assert not any(c[0] == "write_table" for c in fake.calls)
    assert fake.calls[-1][3] == "FAILED"
