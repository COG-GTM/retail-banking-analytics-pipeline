"""Parity and unit tests for synapse/spark/jobs/master_profile.py."""

import os
import sys
import types

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from synapse.spark.jobs import master_profile as mp  # noqa: E402

AS_OF = date(2026, 4, 10)
DATA = REPO_ROOT / "data"

ADD_OPENS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_master_profile")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", ADD_OPENS)
        .config("spark.executor.extraJavaOptions", ADD_OPENS)
        .getOrCreate()
    )
    yield spark
    spark.stop()


# --- CSV-native schemas (committed CSVs write e.g. active_accounts as "3.0") ---

STG_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("first_name", T.StringType(), True),
        T.StructField("last_name", T.StringType(), True),
        T.StructField("date_of_birth", T.DateType(), True),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("customer_since", T.DateType(), True),
        T.StructField("tenure_months", T.IntegerType(), True),
        T.StructField("customer_status", T.StringType(), True),
        T.StructField("segment_code", T.StringType(), True),
        T.StructField("branch_id", T.IntegerType(), True),
        T.StructField("primary_address", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("state_code", T.StringType(), True),
        T.StructField("zip_code", T.StringType(), True),
        T.StructField("num_accounts", T.IntegerType(), True),
        T.StructField("num_active_accounts", T.DoubleType(), True),
        T.StructField("has_checking", T.StringType(), True),
        T.StructField("has_savings", T.StringType(), True),
        T.StructField("has_credit", T.StringType(), True),
        T.StructField("has_loan", T.StringType(), True),
        T.StructField("total_balance", T.DoubleType(), True),
        T.StructField("total_credit_limit", T.DoubleType(), True),
        T.StructField("credit_utilization_pct", T.DoubleType(), True),
        T.StructField("load_ts", T.StringType(), True),
    ]
)

SEG_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("segment_name", T.StringType(), True),
        T.StructField("segment_id", T.IntegerType(), True),
        T.StructField("subsegment_id", T.IntegerType(), True),
        T.StructField("lifetime_value_score", T.DoubleType(), True),
        T.StructField("engagement_score", T.DoubleType(), True),
        T.StructField("digital_adoption_score", T.DoubleType(), True),
        T.StructField("product_breadth_index", T.DoubleType(), True),
        T.StructField("tenure_group", T.StringType(), True),
        T.StructField("age_group", T.StringType(), True),
        T.StructField("balance_tier", T.StringType(), True),
        T.StructField("channel_preference", T.StringType(), True),
        T.StructField("cross_sell_flag", T.StringType(), True),
        T.StructField("upsell_flag", T.StringType(), True),
        T.StructField("retention_risk_flag", T.StringType(), True),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("effective_date", T.DateType(), True),
        T.StructField("load_ts", T.StringType(), True),
    ]
)

TXN_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("total_accounts", T.IntegerType(), True),
        T.StructField("active_accounts", T.IntegerType(), True),
        T.StructField("total_transactions", T.IntegerType(), True),
        T.StructField("total_debit_amt", T.DoubleType(), True),
        T.StructField("total_credit_amt", T.DoubleType(), True),
        T.StructField("total_fees", T.DoubleType(), True),
        T.StructField("top_spend_category", T.StringType(), True),
        T.StructField("net_cash_flow", T.DoubleType(), True),
        T.StructField("avg_transaction_size", T.DoubleType(), True),
        T.StructField("digital_txn_pct", T.DoubleType(), True),
        T.StructField("monthly_spend_trend", T.StringType(), True),
        T.StructField("fee_income", T.DoubleType(), True),
        T.StructField("interest_income", T.DoubleType(), True),
        T.StructField("revenue_contribution", T.DoubleType(), True),
        T.StructField("spend_percentile", T.DoubleType(), True),
        T.StructField("anomaly_flag", T.StringType(), True),
        T.StructField("reporting_period", T.StringType(), True),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("effective_date", T.DateType(), True),
        T.StructField("load_ts", T.StringType(), True),
    ]
)

RISK_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("composite_risk_score", T.DoubleType(), True),
        T.StructField("risk_tier", T.StringType(), True),
        T.StructField("probability_of_default", T.DoubleType(), True),
        T.StructField("credit_risk_component", T.DoubleType(), True),
        T.StructField("behaviour_risk_component", T.DoubleType(), True),
        T.StructField("velocity_risk_component", T.DoubleType(), True),
        T.StructField("bureau_score_component", T.DoubleType(), True),
        T.StructField("payment_history_component", T.DoubleType(), True),
        T.StructField("primary_risk_driver", T.StringType(), True),
        T.StructField("secondary_risk_driver", T.StringType(), True),
        T.StructField("score_delta_30d", T.DoubleType(), True),
        T.StructField("watch_list_flag", T.StringType(), True),
        T.StructField("review_required_flag", T.StringType(), True),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("effective_date", T.DateType(), True),
        T.StructField("load_ts", T.StringType(), True),
    ]
)

EXPECTED_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("full_name", T.StringType(), True),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("state_code", T.StringType(), True),
        T.StructField("customer_since", T.DateType(), True),
        T.StructField("tenure_months", T.IntegerType(), True),
        T.StructField("customer_status", T.StringType(), True),
        T.StructField("segment_name", T.StringType(), True),
        T.StructField("lifetime_value_score", T.DoubleType(), True),
        T.StructField("engagement_score", T.DoubleType(), True),
        T.StructField("total_accounts", T.IntegerType(), True),
        T.StructField("active_accounts", T.DoubleType(), True),
        T.StructField("total_balance", T.DoubleType(), True),
        T.StructField("total_credit_limit", T.DoubleType(), True),
        T.StructField("credit_utilization_pct", T.DoubleType(), True),
        T.StructField("monthly_transactions", T.IntegerType(), True),
        T.StructField("monthly_spend", T.DoubleType(), True),
        T.StructField("net_cash_flow", T.DoubleType(), True),
        T.StructField("top_spend_category", T.StringType(), True),
        T.StructField("digital_txn_pct", T.DoubleType(), True),
        T.StructField("composite_risk_score", T.DoubleType(), True),
        T.StructField("risk_tier", T.StringType(), True),
        T.StructField("probability_of_default", T.DoubleType(), True),
        T.StructField("watch_list_flag", T.StringType(), True),
        T.StructField("cross_sell_flag", T.StringType(), True),
        T.StructField("upsell_flag", T.StringType(), True),
        T.StructField("retention_risk_flag", T.StringType(), True),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("effective_date", T.DateType(), True),
        T.StructField("load_ts", T.StringType(), True),
    ]
)


def _upper(df):
    return df.select([F.col(c).alias(c.upper()) for c in df.columns])


def _load(spark, path, schema):
    return spark.read.schema(schema).option("header", True).csv(str(path))


@pytest.fixture(scope="module")
def inputs(spark):
    stg = _upper(_load(spark, DATA / "02_bteq_staging" / "stg_customer_360.csv", STG_SCHEMA))
    stg = stg.select(
        *[
            F.col(c).cast(t).alias(c)
            for c, t in {
                "AGE": "short",
                "NUM_ACCOUNTS": "short",
                "NUM_ACTIVE_ACCOUNTS": "short",
                "TOTAL_BALANCE": "decimal(18,2)",
                "TOTAL_CREDIT_LIMIT": "decimal(18,2)",
                "CREDIT_UTILIZATION_PCT": "decimal(5,2)",
            }.items()
        ],
        *[c for c in stg.columns if c not in {
            "AGE", "NUM_ACCOUNTS", "NUM_ACTIVE_ACCOUNTS", "TOTAL_BALANCE",
            "TOTAL_CREDIT_LIMIT", "CREDIT_UTILIZATION_PCT",
        }],
    )

    seg = _upper(_load(spark, DATA / "03_sas_data_products" / "customer_segments.csv", SEG_SCHEMA))
    seg = seg.select(
        F.col("CUSTOMER_ID"), F.col("SEGMENT_NAME"),
        F.col("LIFETIME_VALUE_SCORE").cast("decimal(10,2)").alias("LIFETIME_VALUE_SCORE"),
        F.col("ENGAGEMENT_SCORE").cast("decimal(5,2)").alias("ENGAGEMENT_SCORE"),
        F.col("CROSS_SELL_FLAG"), F.col("UPSELL_FLAG"), F.col("RETENTION_RISK_FLAG"),
    )

    txn = _upper(_load(spark, DATA / "03_sas_data_products" / "transaction_analytics.csv", TXN_SCHEMA))
    txn = txn.select(
        F.col("CUSTOMER_ID"),
        F.col("TOTAL_TRANSACTIONS").cast("int").alias("TOTAL_TRANSACTIONS"),
        F.col("TOTAL_DEBIT_AMT").cast("decimal(18,2)").alias("TOTAL_DEBIT_AMT"),
        F.col("NET_CASH_FLOW").cast("decimal(18,2)").alias("NET_CASH_FLOW"),
        F.col("TOP_SPEND_CATEGORY"),
        F.col("DIGITAL_TXN_PCT").cast("decimal(5,2)").alias("DIGITAL_TXN_PCT"),
        F.col("REPORTING_PERIOD"), F.col("EFFECTIVE_DATE"),
    )

    risk = _upper(_load(spark, DATA / "03_sas_data_products" / "customer_risk_scores.csv", RISK_SCHEMA))
    risk = risk.select(
        F.col("CUSTOMER_ID"),
        F.col("COMPOSITE_RISK_SCORE").cast("decimal(6,2)").alias("COMPOSITE_RISK_SCORE"),
        F.col("RISK_TIER"),
        F.col("PROBABILITY_OF_DEFAULT").cast("decimal(7,6)").alias("PROBABILITY_OF_DEFAULT"),
        F.col("WATCH_LIST_FLAG"),
    )

    return {
        ("stg", "STG_CUSTOMER_360"): stg,
        ("dp", "CUSTOMER_SEGMENTS"): seg,
        ("dp", "TRANSACTION_ANALYTICS"): txn,
        ("dp", "CUSTOMER_RISK_SCORES"): risk,
    }


@pytest.fixture(scope="module")
def expected(spark):
    return _load(
        spark, DATA / "03_sas_data_products" / "customer_master_profile.csv",
        EXPECTED_SCHEMA,
    )


@pytest.fixture(scope="module")
def actual(inputs):
    return mp.transform(
        inputs[("stg", "STG_CUSTOMER_360")],
        inputs[("dp", "CUSTOMER_SEGMENTS")],
        inputs[("dp", "TRANSACTION_ANALYTICS")],
        inputs[("dp", "CUSTOMER_RISK_SCORES")],
        AS_OF,
    ).cache()


def test_parity(actual, expected):
    assert actual.columns == [f.name for f in mp.OUTPUT_SCHEMA.fields]
    assert [f.dataType for f in actual.schema.fields] == [
        f.dataType for f in mp.OUTPUT_SCHEMA.fields
    ]
    assert actual.count() == 407

    exp = expected.withColumnRenamed("load_ts", "LOAD_TS_EXPECTED")
    joined = actual.join(exp, actual.CUSTOMER_ID == exp.customer_id, "inner")
    assert joined.count() == 407, "CUSTOMER_ID sets differ"
    assert set(r.CUSTOMER_ID for r in actual.select("CUSTOMER_ID").collect()) == set(
        r.customer_id for r in expected.collect()
    )

    numeric = {
        "age", "tenure_months", "lifetime_value_score", "engagement_score",
        "total_accounts", "active_accounts", "total_balance",
        "total_credit_limit", "credit_utilization_pct", "monthly_transactions",
        "monthly_spend", "net_cash_flow", "digital_txn_pct",
        "composite_risk_score", "probability_of_default",
    }
    results = []
    for col in expected.columns:
        if col in ("customer_id", "load_ts"):
            continue
        a, e = actual[col.upper()], exp[col]
        both_null = a.isNull() & e.isNull()
        one_null = a.isNull() != e.isNull()
        if col in numeric:
            diff = F.abs(a.cast("double") - e)
            mismatch = (diff > 0.01) | one_null
            maxdiff = joined.where(~both_null).agg(F.max(diff)).first()[0]
            mism = joined.where(mismatch).count()
            results.append((col, maxdiff, mism))
        else:
            mism = joined.where((a != e) | one_null).count()
            results.append((col, None, mism))

    print("\n%-26s %12s %8s" % ("column", "max_abs_diff", "mismatches"))
    bad = []
    for col, maxdiff, mism in results:
        print("%-26s %12s %8d" % (col, maxdiff, mism))
        if mism:
            bad.append(col)
    assert not bad, f"columns with mismatches: {bad}"

    assert actual.where(F.col("MODEL_VERSION") != "MASTER_V1.5").count() == 0
    assert actual.where(F.col("EFFECTIVE_DATE") != AS_OF).count() == 0
    assert actual.where(F.col("LOAD_TS").isNull()).count() == 0


def _synthetic(spark):
    stg_schema = T.StructType([
        T.StructField("CUSTOMER_ID", T.LongType(), True),
        T.StructField("FIRST_NAME", T.StringType(), True),
        T.StructField("LAST_NAME", T.StringType(), True),
        T.StructField("AGE", T.ShortType(), True),
        T.StructField("STATE_CODE", T.StringType(), True),
        T.StructField("CUSTOMER_SINCE", T.DateType(), True),
        T.StructField("TENURE_MONTHS", T.IntegerType(), True),
        T.StructField("CUSTOMER_STATUS", T.StringType(), True),
        T.StructField("NUM_ACCOUNTS", T.ShortType(), True),
        T.StructField("NUM_ACTIVE_ACCOUNTS", T.ShortType(), True),
        T.StructField("TOTAL_BALANCE", T.DoubleType(), True),
        T.StructField("TOTAL_CREDIT_LIMIT", T.DoubleType(), True),
        T.StructField("CREDIT_UTILIZATION_PCT", T.DoubleType(), True),
    ])
    stg = spark.createDataFrame(
        [
            (1, "Ann  ", "Lee ", 30, "TX", date(2020, 1, 1), 60, "A", 2, 1,
             100.0, 500.0, 20.0),
            (2, "Bob", "Smith", 40, "CA", date(2019, 1, 1), 70, "A", 1, 1,
             50.0, 0.0, 0.0),
            (3, "Cal", "Jones", 50, "NY", date(2018, 1, 1), 80, "I", 1, 0,
             10.0, 0.0, 0.0),
            (5, "Dot", "Ng", 35, "WA", date(2021, 1, 1), 40, "A", 3, 2,
             200.0, 100.0, 50.0),
            (6, "Eli", "Fox", 45, "FL", date(2017, 1, 1), 90, "A", 1, 1,
             75.0, 25.0, 10.0),
        ],
        stg_schema,
    )
    seg = spark.createDataFrame(
        [
            (1, "PREMIUM", 900.0, 80.0, "Y", "N", "N"),
            (4, "GHOST", 1.0, 1.0, "N", "N", "N"),
            (5, "BASIC", None, 10.0, "N", "N", "Y"),
            (6, "BASIC", 5.0, 5.0, "N", "N", "N"),
        ],
        T.StructType([
            T.StructField("CUSTOMER_ID", T.LongType(), True),
            T.StructField("SEGMENT_NAME", T.StringType(), True),
            T.StructField("LIFETIME_VALUE_SCORE", T.DoubleType(), True),
            T.StructField("ENGAGEMENT_SCORE", T.DoubleType(), True),
            T.StructField("CROSS_SELL_FLAG", T.StringType(), True),
            T.StructField("UPSELL_FLAG", T.StringType(), True),
            T.StructField("RETENTION_RISK_FLAG", T.StringType(), True),
        ]),
    )
    txn = spark.createDataFrame(
        [
            (1, 10, 500.0, -100.0, "TRAVEL", 60.0, "2026-04", date(2026, 4, 10)),
            (4, 1, 1.0, 1.0, "X", 1.0, "2026-04", date(2026, 4, 10)),
            (5, 2, 20.0, 5.0, "GROCERY", 30.0, "2026-04", date(2026, 4, 10)),
            # stale row for cust 6: wrong EFFECTIVE_DATE -> txn defaults
            (6, 9, 99.0, 9.0, "DINING", 90.0, "2026-04", date(2026, 4, 9)),
        ],
        T.StructType([
            T.StructField("CUSTOMER_ID", T.LongType(), True),
            T.StructField("TOTAL_TRANSACTIONS", T.IntegerType(), True),
            T.StructField("TOTAL_DEBIT_AMT", T.DoubleType(), True),
            T.StructField("NET_CASH_FLOW", T.DoubleType(), True),
            T.StructField("TOP_SPEND_CATEGORY", T.StringType(), True),
            T.StructField("DIGITAL_TXN_PCT", T.DoubleType(), True),
            T.StructField("REPORTING_PERIOD", T.StringType(), True),
            T.StructField("EFFECTIVE_DATE", T.DateType(), True),
        ]),
    )
    risk = spark.createDataFrame(
        [
            (1, 15.5, "LOW", 0.01, "N"),
            (4, 99.0, "HIGH", 0.9, "Y"),
            (5, 50.0, "MODERATE", 0.2, "Y"),
            (6, 10.0, "LOW", 0.05, "N"),
        ],
        T.StructType([
            T.StructField("CUSTOMER_ID", T.LongType(), True),
            T.StructField("COMPOSITE_RISK_SCORE", T.DoubleType(), True),
            T.StructField("RISK_TIER", T.StringType(), True),
            T.StructField("PROBABILITY_OF_DEFAULT", T.DoubleType(), True),
            T.StructField("WATCH_LIST_FLAG", T.StringType(), True),
        ]),
    )
    # DoubleType inputs: transform's final OUTPUT_SCHEMA casts normalize types
    return stg, seg, txn, risk


def test_merge_defaults(spark):
    stg, seg, txn, risk = _synthetic(spark)
    rows = {
        r.CUSTOMER_ID: r
        for r in mp.transform(stg, seg, txn, risk, AS_OF).collect()
    }
    assert set(rows) == {1, 2, 5, 6}  # cust 3 'I' dropped, cust 4 not in base

    r1 = rows[1]
    assert r1.FULL_NAME == "Ann Lee"
    assert r1.SEGMENT_NAME == "PREMIUM"
    assert float(r1.MONTHLY_SPEND) == 500.0
    assert r1.RISK_TIER == "LOW"
    assert r1.CROSS_SELL_FLAG == "Y"

    r2 = rows[2]
    assert r2.SEGMENT_NAME == "UNCLASSIFIED"
    assert float(r2.LIFETIME_VALUE_SCORE) == 0
    assert float(r2.ENGAGEMENT_SCORE) == 0
    assert r2.CROSS_SELL_FLAG == "N" and r2.UPSELL_FLAG == "N"
    assert r2.RETENTION_RISK_FLAG == "N"
    assert r2.MONTHLY_TRANSACTIONS == 0
    assert float(r2.MONTHLY_SPEND) == 0
    assert float(r2.NET_CASH_FLOW) == 0
    assert r2.TOP_SPEND_CATEGORY is None
    assert float(r2.DIGITAL_TXN_PCT) == 0
    assert r2.COMPOSITE_RISK_SCORE is None
    assert r2.RISK_TIER == "UNKNOWN"
    assert r2.PROBABILITY_OF_DEFAULT is None
    assert r2.WATCH_LIST_FLAG == "N"
    assert r2.MODEL_VERSION == "MASTER_V1.5"
    assert r2.EFFECTIVE_DATE == AS_OF

    r5 = rows[5]
    assert r5.SEGMENT_NAME == "BASIC"
    assert r5.LIFETIME_VALUE_SCORE is None  # matched row keeps NULL, not coalesced

    r6 = rows[6]
    assert r6.MONTHLY_TRANSACTIONS == 0  # stale txn row filtered -> defaults
    assert r6.TOP_SPEND_CATEGORY is None


def test_quality_report(spark, actual):
    exp = pd.read_csv(DATA / "03_sas_data_products" / "customer_master_profile.csv")
    want = {
        "TOTAL": 407,
        "HAS_SEGMENT": int((exp.segment_name != "UNCLASSIFIED").sum()),
        "HAS_TXN": int((exp.monthly_transactions > 0).sum()),
        "HAS_RISK_SCORE": int((exp.risk_tier != "UNKNOWN").sum()),
        "CROSS_SELL_ELIGIBLE": int((exp.cross_sell_flag == "Y").sum()),
        "UPSELL_ELIGIBLE": int((exp.upsell_flag == "Y").sum()),
        "RETENTION_AT_RISK": int((exp.retention_risk_flag == "Y").sum()),
        "ON_WATCH_LIST": int((exp.watch_list_flag == "Y").sum()),
    }
    got = mp.quality_report(actual).collect()[0].asDict()
    print("\nquality:", got)
    assert got == want
    assert all(
        isinstance(f.dataType, T.LongType) for f in mp.quality_report(actual).schema
    )

    stg, seg, txn, risk = _synthetic(spark)
    syn = mp.transform(stg, seg, txn, risk, AS_OF)
    got_syn = mp.quality_report(syn).collect()[0].asDict()
    # cust 1 has seg+txn+risk, cust 5 has seg(null ltv)+txn+risk watch=Y,
    # cust 6 seg+risk only, cust 2 none.
    assert got_syn == {
        "TOTAL": 4,
        "HAS_SEGMENT": 3,
        "HAS_TXN": 2,
        "HAS_RISK_SCORE": 3,
        "CROSS_SELL_ELIGIBLE": 1,
        "UPSELL_ELIGIBLE": 0,
        "RETENTION_AT_RISK": 1,
        "ON_WATCH_LIST": 1,
    }


def _fake_pu(inputs, log_steps, writes, validates, fail_validate=False):
    class ValidationError(Exception):
        pass

    mod_common = types.ModuleType("synapse.spark.common")
    mod_pu = types.ModuleType("synapse.spark.common.pipeline_utils")
    mod_pu.ValidationError = ValidationError
    mod_pu.get_run_params = lambda argv: types.SimpleNamespace(
        as_of_date=AS_OF, env="dev"
    )
    mod_pu.read_table = lambda spark, schema, table: inputs[(schema, table)]

    def write_table(df, schema, table, mode="overwrite", pre_delete_where=None):
        writes.append((df, schema, table, mode, df.count()))

    def log_step(spark, job_name, step_name, status, row_count=None, msg=None):
        log_steps.append((status, row_count, msg))

    def validate_table(df, key_cols, not_null_cols, min_rows=1):
        validates.append((key_cols, not_null_cols, min_rows))
        if fail_validate:
            raise ValidationError("dup keys")

    mod_pu.write_table = write_table
    mod_pu.log_step = log_step
    mod_pu.validate_table = validate_table
    mod_common.pipeline_utils = mod_pu
    return mod_common, mod_pu


def _install_fake_pu(monkeypatch, mod_common, mod_pu):
    monkeypatch.setitem(sys.modules, "synapse.spark.common", mod_common)
    monkeypatch.setitem(
        sys.modules, "synapse.spark.common.pipeline_utils", mod_pu
    )


def test_main(monkeypatch, spark, inputs):
    log_steps, writes, validates = [], [], []
    mod_common, mod_pu = _fake_pu(inputs, log_steps, writes, validates)
    _install_fake_pu(monkeypatch, mod_common, mod_pu)

    assert mp.main([]) == 0
    assert len(writes) == 1
    df, schema, table, mode, n = writes[0]
    assert (schema, table, mode) == ("dp", "CUSTOMER_MASTER_PROFILE", "overwrite")
    assert n == 407
    assert validates == [
        (["CUSTOMER_ID"], ["CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"], 1)
    ]
    assert log_steps[-1][0] == "SUCCESS"
    assert log_steps[-1][2] == "Full pipeline complete"
    assert not any(s == "FAILED" for s, _, _ in log_steps)


def test_main_validation_failure(monkeypatch, spark, inputs):
    log_steps, writes, validates = [], [], []
    mod_common, mod_pu = _fake_pu(
        inputs, log_steps, writes, validates, fail_validate=True
    )
    _install_fake_pu(monkeypatch, mod_common, mod_pu)

    assert mp.main([]) == 1
    assert writes == []
    assert any(s == "FAILED" for s, _, _ in log_steps)
