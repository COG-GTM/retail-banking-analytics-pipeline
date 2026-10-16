import csv
import math
import os
import sys
import types
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
pyspark = pytest.importorskip("pyspark")

import numpy as np
import pandas as pd

from pyspark.ml import feature
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from synapse.spark.jobs import risk_scoring

AS_OF = date(2026, 4, 10)
RISK_DDL = [
    ("CUSTOMER_ID", "BIGINT"),
    ("ACCOUNT_OVERDRAFT_CNT", "INTEGER"),
    ("NSF_FEE_TOTAL", "DECIMAL(15,2)"),
    ("LARGE_WITHDRAWAL_CNT", "INTEGER"),
    ("LARGE_WITHDRAWAL_AMT", "DECIMAL(18,2)"),
    ("AVG_DAILY_BALANCE_30D", "DECIMAL(15,2)"),
    ("AVG_DAILY_BALANCE_90D", "DECIMAL(15,2)"),
    ("BALANCE_VOLATILITY", "DECIMAL(10,4)"),
    ("CREDIT_UTIL_RATIO", "DECIMAL(5,4)"),
    ("PAYMENT_ONTIME_PCT", "DECIMAL(5,2)"),
    ("PAYMENT_LATE_CNT", "INTEGER"),
    ("MONTHS_SINCE_LAST_LATE", "INTEGER"),
    ("EXTERNAL_CREDIT_SCORE", "INTEGER"),
    ("DEBIT_VELOCITY_7D", "DECIMAL(15,2)"),
    ("DEBIT_VELOCITY_30D", "DECIMAL(15,2)"),
    ("NEW_MERCHANT_CNT_30D", "INTEGER"),
    ("INTERNATIONAL_TXN_CNT", "INTEGER"),
    ("HIGH_RISK_MERCHANT_CNT", "INTEGER"),
    ("LOAD_TS", "TIMESTAMP"),
]
CUSTOMER_DDL = [
    ("CUSTOMER_ID", "BIGINT"),
    ("FIRST_NAME", "VARCHAR"),
    ("LAST_NAME", "VARCHAR"),
    ("DATE_OF_BIRTH", "DATE"),
    ("AGE", "SMALLINT"),
    ("CUSTOMER_SINCE", "DATE"),
    ("TENURE_MONTHS", "INTEGER"),
    ("CUSTOMER_STATUS", "CHAR"),
    ("SEGMENT_CODE", "VARCHAR"),
    ("BRANCH_ID", "INTEGER"),
    ("PRIMARY_ADDRESS", "VARCHAR"),
    ("CITY", "VARCHAR"),
    ("STATE_CODE", "CHAR"),
    ("ZIP_CODE", "VARCHAR"),
    ("NUM_ACCOUNTS", "SMALLINT"),
    ("NUM_ACTIVE_ACCOUNTS", "SMALLINT"),
    ("HAS_CHECKING", "CHAR"),
    ("HAS_SAVINGS", "CHAR"),
    ("HAS_CREDIT", "CHAR"),
    ("HAS_LOAN", "CHAR"),
    ("TOTAL_BALANCE", "DECIMAL(18,2)"),
    ("TOTAL_CREDIT_LIMIT", "DECIMAL(18,2)"),
    ("CREDIT_UTILIZATION_PCT", "DECIMAL(5,2)"),
    ("LOAD_TS", "TIMESTAMP"),
]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-risk-scoring")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _read_and_cast(spark, relative_path, ddl):
    path = REPO_ROOT / relative_path
    with path.open(newline="") as file:
        header = next(csv.reader(file))
    assert [name.upper() for name in header] == [name for name, _ in ddl]

    raw_schema = StructType(
        [StructField(name, StringType(), True) for name, _ in ddl]
    )
    dataframe = spark.read.option("header", "true").schema(raw_schema).csv(str(path))
    for name, data_type in ddl:
        if data_type.startswith("DECIMAL"):
            precision, scale = map(
                int, data_type.removeprefix("DECIMAL(").removesuffix(")").split(",")
            )
            dataframe = dataframe.withColumn(
                name,
                dataframe[name].cast(DoubleType()).cast(DecimalType(precision, scale)),
            )
        elif data_type == "BIGINT":
            dataframe = dataframe.withColumn(
                name, dataframe[name].cast(DoubleType()).cast(LongType())
            )
        elif data_type == "INTEGER":
            dataframe = dataframe.withColumn(
                name, dataframe[name].cast(DoubleType()).cast(IntegerType())
            )
        elif data_type == "SMALLINT":
            dataframe = dataframe.withColumn(
                name, dataframe[name].cast(DoubleType()).cast(ShortType())
            )
        elif data_type == "DATE":
            dataframe = dataframe.withColumn(name, dataframe[name].cast(DateType()))
        elif data_type in ("CHAR", "VARCHAR"):
            dataframe = dataframe.withColumn(name, dataframe[name].cast(StringType()))
        elif data_type == "TIMESTAMP":
            dataframe = dataframe.withColumn(name, dataframe[name].cast(StringType()))
    return dataframe


@pytest.fixture(scope="session")
def inputs(spark):
    return (
        _read_and_cast(spark, "data/02_bteq_staging/stg_risk_factors.csv", RISK_DDL),
        _read_and_cast(spark, "data/02_bteq_staging/stg_customer_360.csv", CUSTOMER_DDL),
    )


@pytest.fixture(scope="session")
def expected():
    return pd.read_csv(REPO_ROOT / "data/03_sas_data_products/customer_risk_scores.csv")


@pytest.fixture(scope="module")
def transformed_df(spark, inputs):
    return risk_scoring.transform(*inputs, AS_OF)


@pytest.fixture(scope="module")
def transformed(transformed_df):
    result = (
        transformed_df.withColumn(
            "EFFECTIVE_DATE", transformed_df["EFFECTIVE_DATE"].cast("string")
        )
        .withColumn("LOAD_TS", transformed_df["LOAD_TS"].cast("string"))
        .toPandas()
    )
    result["CUSTOMER_ID"] = result["CUSTOMER_ID"].astype("int64")
    return result.sort_values("CUSTOMER_ID").reset_index(drop=True)


@pytest.fixture(scope="module")
def expected_indexed(expected):
    frame = expected.copy()
    frame["customer_id"] = frame["customer_id"].astype("int64")
    return frame.set_index("customer_id").sort_index()


def _difference(actual, expected, output_name, expected_name=None):
    expected_name = expected_name or output_name.lower()
    left = pd.to_numeric(actual[output_name], errors="coerce")
    right = pd.to_numeric(expected[expected_name], errors="coerce")
    return (left - right).abs()


def test_output_schema(transformed_df):
    assert [(field.name, field.dataType) for field in transformed_df.schema] == [
        (field.name, field.dataType) for field in risk_scoring.OUTPUT_SCHEMA
    ]
def test_row_count_and_customer_ids(transformed, expected):
    assert len(transformed) == 407
    assert set(transformed.CUSTOMER_ID) == set(expected.customer_id.astype("int64"))


def test_numeric_parity(transformed, expected_indexed):
    actual = transformed.set_index("CUSTOMER_ID").sort_index()
    expected = expected_indexed
    columns = [
        "CREDIT_RISK_COMPONENT",
        "BEHAVIOUR_RISK_COMPONENT",
        "VELOCITY_RISK_COMPONENT",
        "BUREAU_SCORE_COMPONENT",
        "PAYMENT_HISTORY_COMPONENT",
        "COMPOSITE_RISK_SCORE",
        "SCORE_DELTA_30D",
    ]
    for column in columns:
        diff = (pd.to_numeric(actual[column]) - pd.to_numeric(expected[column.lower()])).abs()
        assert diff.max() <= 0.01, f"{column} max abs diff: {diff.max()}"


def test_categorical_parity_and_effective_date(transformed, expected_indexed):
    actual = transformed.set_index("CUSTOMER_ID").sort_index()
    expected = expected_indexed
    for column in (
        "RISK_TIER",
        "PRIMARY_RISK_DRIVER",
        "SECONDARY_RISK_DRIVER",
        "WATCH_LIST_FLAG",
        "REVIEW_REQUIRED_FLAG",
        "MODEL_VERSION",
    ):
        assert (actual[column].to_numpy() == expected[column.lower()].to_numpy()).all()
    assert (actual["EFFECTIVE_DATE"] == AS_OF.isoformat()).all()


def test_probability_of_default_semantics(transformed, expected_indexed):
    values = pd.to_numeric(transformed["PROBABILITY_OF_DEFAULT"])
    assert (values == 0.0).all()
    assert not values.isna().any()
    assert np.isfinite(values).all()
    expected_pd = pd.to_numeric(expected_indexed["probability_of_default"])
    mae = np.abs(values.to_numpy() - expected_pd.to_numpy()).mean()
    correlation = (
        np.corrcoef(values, expected_pd)[0, 1]
        if values.nunique() > 1 and expected_pd.nunique() > 1
        else float("nan")
    )
    print(f"PD MAE={mae:.8f}; Pearson corr={correlation}")
    assert mae <= 0.05 + 1e-9


def _numpy_irls(features, labels):
    design = np.column_stack([np.ones(len(features)), features])
    beta = np.zeros(design.shape[1])
    for _ in range(100):
        logits = np.clip(design @ beta, -40, 40)
        probabilities = 1 / (1 + np.exp(-logits))
        weights = probabilities * (1 - probabilities)
        gradient = design.T @ (labels - probabilities)
        hessian = design.T @ (weights[:, None] * design)
        step = np.linalg.solve(hessian, gradient)
        beta += step
        if np.max(np.abs(step)) < 1e-12:
            break
    return beta


def test_non_degenerate_logistic_regression_matches_numpy_mle(spark):
    rng = np.random.default_rng(42)
    features = rng.normal(size=(2000, 3))
    true_beta = np.array([-0.2, 0.8, -1.2, 0.6])
    logits = true_beta[0] + features @ true_beta[1:]
    probabilities = 1 / (1 + np.exp(-logits))
    labels = rng.binomial(1, probabilities).astype(float)
    names = ["X1", "X2", "X3"]
    rows = [
        tuple(float(value) for value in row) + (float(label),)
        for row, label in zip(features, labels)
    ]
    frame = spark.createDataFrame(rows, names + ["DEFAULT_FLAG"])
    assembled = feature.VectorAssembler(
        inputCols=names, outputCol="features", handleInvalid="skip"
    ).transform(frame)
    model = risk_scoring._build_logistic_regression().fit(
        assembled.select("features", "DEFAULT_FLAG")
    )
    mle = _numpy_irls(features, labels)
    coefficient_diffs = np.asarray(model.coefficients) - mle[1:]
    intercept_diff = float(model.intercept) - mle[0]
    spark_probability = np.asarray(
        assembled.withColumn("PD", risk_scoring._pd_column(model, names))
        .select("PD")
        .toPandas()["PD"]
    )
    mle_probability = 1 / (1 + np.exp(-(mle[0] + features @ mle[1:])))
    corr = np.corrcoef(spark_probability, mle_probability)[0, 1]
    mae = np.abs(spark_probability - mle_probability).mean()
    print(
        "Synthetic MLE coefficient max abs diff="
        f"{np.max(np.abs(coefficient_diffs)):.10g}, intercept abs diff="
        f"{abs(intercept_diff):.10g}, probability corr={corr:.10g}, MAE={mae:.10g}"
    )
    assert np.max(np.abs(coefficient_diffs)) < 1e-3
    assert abs(intercept_diff) < 1e-3
    assert corr >= 0.9999
    assert mae <= 1e-4


@pytest.mark.parametrize("label,expected_pd", [(0.0, 0.0), (1.0, 0.0)])
def test_single_class_logistic_regression_is_finite(spark, label, expected_pd):
    names = ["X1", "X2"]
    rows = [(float(index), float(index % 4), label) for index in range(20)]
    frame = spark.createDataFrame(rows, names + ["DEFAULT_FLAG"])
    assembled = feature.VectorAssembler(
        inputCols=names, outputCol="features", handleInvalid="skip"
    ).transform(frame)
    model = risk_scoring._build_logistic_regression().fit(
        assembled.select("features", "DEFAULT_FLAG")
    )
    print(
        f"Single-class label={label}: intercept={model.intercept}, "
        f"coefficients={list(model.coefficients)}"
    )
    values = (
        assembled.withColumn("PD", risk_scoring._pd_column(model, names))
        .select("PD")
        .toPandas()["PD"]
    )
    assert np.isfinite(values).all()
    assert (values == expected_pd).all()


def test_null_payment_rate_uses_sas_missing_clip(inputs):
    risk_factors, customer_360 = inputs
    model_candidates = (
        risk_factors.join(
            customer_360.select("CUSTOMER_ID", "TENURE_MONTHS", "CUSTOMER_STATUS"),
            on="CUSTOMER_ID",
        )
        .filter(F.col("CUSTOMER_STATUS") == "A")
        .filter(
            F.col("CREDIT_UTIL_RATIO").isNotNull()
            & F.col("PAYMENT_ONTIME_PCT").isNotNull()
            & F.col("BALANCE_VOLATILITY").isNotNull()
            & F.col("ACCOUNT_OVERDRAFT_CNT").isNotNull()
            & F.col("LARGE_WITHDRAWAL_CNT").isNotNull()
            & F.col("HIGH_RISK_MERCHANT_CNT").isNotNull()
            & F.col("TENURE_MONTHS").isNotNull()
            & (
                F.col("DEBIT_VELOCITY_30D").isNull()
                | (F.col("DEBIT_VELOCITY_30D") <= 0)
                | F.col("DEBIT_VELOCITY_7D").isNotNull()
            )
        )
        .select("CUSTOMER_ID")
        .limit(2)
        .collect()
    )
    customer_ids = [row.CUSTOMER_ID for row in model_candidates]
    assert len(customer_ids) == 2
    missing_rate_id = customer_ids[0]
    payment_rate_type = risk_factors.schema["PAYMENT_ONTIME_PCT"].dataType
    tiny_risk_factors = risk_factors.filter(
        F.col("CUSTOMER_ID").isin(customer_ids)
    ).withColumn(
        "PAYMENT_ONTIME_PCT",
        F.when(
            F.col("CUSTOMER_ID") == missing_rate_id,
            F.lit(None).cast(payment_rate_type),
        ).otherwise(F.col("PAYMENT_ONTIME_PCT")),
    )
    tiny_customer_360 = customer_360.filter(
        F.col("CUSTOMER_ID").isin(customer_ids)
    )

    result = risk_scoring.transform(
        tiny_risk_factors, tiny_customer_360, AS_OF
    ).collect()
    assert len(result) == 2
    assert all(row.PROBABILITY_OF_DEFAULT == 0 for row in result)
    row = next(row for row in result if row.CUSTOMER_ID == missing_rate_id)
    assert row.BEHAVIOUR_RISK_COMPONENT == 100
    assert row.PAYMENT_HISTORY_COMPONENT == 100
    expected_composite = round(
        float(row.CREDIT_RISK_COMPONENT) * 0.30
        + float(row.BEHAVIOUR_RISK_COMPONENT) * 0.25
        + float(row.VELOCITY_RISK_COMPONENT) * 0.15
        + (100.0 - float(row.BUREAU_SCORE_COMPONENT)) * 0.20
        + (100.0 - float(row.PAYMENT_HISTORY_COMPONENT)) * 0.10,
        2,
    )
    assert abs(float(row.COMPOSITE_RISK_SCORE) - expected_composite) <= 0.01


def _install_fake_pipeline_utils(
    monkeypatch, read_table, validate, write, log, env="dev"
):
    for name in ("synapse", "synapse.spark", "synapse.spark.common"):
        package = sys.modules.get(name)
        if package is None:
            package = types.ModuleType(name)
            package.__path__ = []
            monkeypatch.setitem(sys.modules, name, package)
    module = types.ModuleType("synapse.spark.common.pipeline_utils")
    module.get_run_params = lambda argv: SimpleNamespace(
        as_of_date=AS_OF,
        env=env,
        risk_score_threshold=700,
        lookback_months=12,
    )
    module.read_table = read_table
    module.validate_table = validate
    module.write_table = write
    module.log_step = log
    monkeypatch.setitem(sys.modules, "synapse.spark.common.pipeline_utils", module)
    monkeypatch.setattr(
        sys.modules["synapse.spark.common"], "pipeline_utils", module, raising=False
    )


def test_main_calls_pipeline_utils_in_order(monkeypatch, inputs):
    read_calls, validate_calls, write_calls, logs, events = [], [], [], [], []
    risk_factors, customer_360 = inputs

    def read_table(spark, schema, table):
        read_calls.append((schema, table))
        return {
            "STG_RISK_FACTORS": risk_factors,
            "STG_CUSTOMER_360": customer_360,
        }[table]

    def validate_table(dataframe, **kwargs):
        events.append("validate")
        validate_calls.append((kwargs, dataframe.count()))

    def write_table(dataframe, schema, table, **kwargs):
        events.append("write")
        write_calls.append((schema, table, kwargs, dataframe.count()))

    def log_step(spark, job, step, status, **kwargs):
        logs.append((job, step, status, kwargs))

    _install_fake_pipeline_utils(monkeypatch, read_table, validate_table, write_table, log_step)
    assert risk_scoring.main(["--as-of-date", "2026-04-10"]) == 0
    assert read_calls == [
        ("stg", "STG_RISK_FACTORS"),
        ("stg", "STG_CUSTOMER_360"),
    ]
    assert validate_calls == [
        (
            {
                "key_cols": ["CUSTOMER_ID"],
                "not_null_cols": ["CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"],
                "min_rows": 1,
            },
            407,
        )
    ]
    assert write_calls == [("dp", "CUSTOMER_RISK_SCORES", {"mode": "overwrite"}, 407)]
    assert events == ["validate", "write"]
    assert logs[-1][2] == "SUCCESS"
    assert logs[-1][3]["row_count"] == 407


def test_main_logs_failed_and_reraises(monkeypatch):
    logs = []

    def read_table(*args):
        raise RuntimeError("source table unavailable")

    _install_fake_pipeline_utils(
        monkeypatch,
        read_table,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda spark, job, step, status, **kwargs: logs.append((status, kwargs)),
    )
    with pytest.raises(RuntimeError, match="source table unavailable"):
        risk_scoring.main([])
    assert logs[-1][0] == "FAILED"
    assert logs[-1][1]["msg"] == "source table unavailable"


@pytest.mark.parametrize("env", ["prod", "PROD", " Prod ", "qa"])
def test_main_prod_min_rows_prevents_write(monkeypatch, inputs, env):
    risk_factors, customer_360 = inputs
    validation_calls, write_calls, logs = [], [], []

    def read_table(spark, schema, table):
        return {
            "STG_RISK_FACTORS": risk_factors,
            "STG_CUSTOMER_360": customer_360,
        }[table]

    def validate_table(dataframe, **kwargs):
        row_count = dataframe.count()
        validation_calls.append((kwargs["min_rows"], row_count))
        if row_count < kwargs["min_rows"]:
            raise ValueError(
                f"table has {row_count} rows; minimum is {kwargs['min_rows']}"
            )

    def write_table(*args, **kwargs):
        write_calls.append((args, kwargs))

    def log_step(spark, job, step, status, **kwargs):
        logs.append((job, step, status, kwargs))

    _install_fake_pipeline_utils(
        monkeypatch,
        read_table,
        validate_table,
        write_table,
        log_step,
        env=env,
    )
    with pytest.raises(ValueError, match="table has 407 rows; minimum is 1000"):
        risk_scoring.main([])
    assert validation_calls == [(1000, 407)]
    assert write_calls == []
    assert logs[-1][2] == "FAILED"


@pytest.mark.parametrize(
    "env,expected",
    [
        ("dev", 1),
        ("DEV", 1),
        ("uat", 1),
        ("UAT", 1),
        ("prod", 1000),
        ("PROD", 1000),
        ("", 1000),
        (None, 1000),
        ("staging", 1000),
    ],
)
def test_min_rows_is_case_insensitive_and_fails_closed(env, expected):
    assert risk_scoring._min_rows(env) == expected


def test_parity_report(transformed, expected, expected_indexed):
    actual = transformed.set_index("CUSTOMER_ID").sort_index()
    expected_frame = expected_indexed
    numeric = [
        "CREDIT_RISK_COMPONENT",
        "BEHAVIOUR_RISK_COMPONENT",
        "VELOCITY_RISK_COMPONENT",
        "BUREAU_SCORE_COMPONENT",
        "PAYMENT_HISTORY_COMPONENT",
        "COMPOSITE_RISK_SCORE",
        "SCORE_DELTA_30D",
    ]
    report = [
        "Risk scoring parity report",
        f"rows: {len(transformed)}",
        f"id-set match: {set(transformed.CUSTOMER_ID) == set(expected.customer_id.astype('int64'))}",
    ]
    for column in numeric:
        diff = (pd.to_numeric(actual[column]) - pd.to_numeric(expected_frame[column.lower()])).abs()
        report.append(f"{column} max abs diff: {diff.max():.8f}")
    for column in (
        "RISK_TIER",
        "PRIMARY_RISK_DRIVER",
        "SECONDARY_RISK_DRIVER",
        "WATCH_LIST_FLAG",
        "REVIEW_REQUIRED_FLAG",
    ):
        agreement = (
            actual[column].to_numpy() == expected_frame[column.lower()].to_numpy()
        ).mean() * 100
        report.append(f"{column} agreement: {agreement:.2f}%")
    pd_mae = (
        pd.to_numeric(actual["PROBABILITY_OF_DEFAULT"])
        - pd.to_numeric(expected_frame["probability_of_default"])
    ).abs().mean()
    actual_pd = pd.to_numeric(actual["PROBABILITY_OF_DEFAULT"]).to_numpy()
    expected_pd = pd.to_numeric(expected_frame["probability_of_default"]).to_numpy()
    pd_corr = (
        np.corrcoef(actual_pd, expected_pd)[0, 1]
        if np.std(actual_pd) and np.std(expected_pd)
        else float("nan")
    )
    report.append(f"PROBABILITY_OF_DEFAULT MAE: {pd_mae:.8f}")
    report.append(f"PROBABILITY_OF_DEFAULT Pearson corr: {pd_corr}")
    report.append(f"our tier distribution: {transformed.RISK_TIER.value_counts().to_dict()}")
    print("\n" + "\n".join(report))
