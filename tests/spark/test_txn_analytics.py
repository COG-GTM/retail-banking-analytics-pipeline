"""Parity tests for synapse/spark/jobs/txn_analytics.py vs the SAS reference
CSV data/03_sas_data_products/transaction_analytics.csv."""

import math
import sys
import types
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from synapse.spark.jobs import txn_analytics as ta

AS_OF = date(2026, 4, 10)
STG_CSV = REPO_ROOT / "data/02_bteq_staging/stg_txn_summary.csv"
REF_CSV = REPO_ROOT / "data/03_sas_data_products/transaction_analytics.csv"
PARITY_TXT = "/tmp/azsyn06_parity.txt"

# staging CSV lowercase name -> (UPPER name, DDL type)
STG_CASTS = {
    "customer_id": ("CUSTOMER_ID", LongType()),
    "account_id": ("ACCOUNT_ID", LongType()),
    "account_type": ("ACCOUNT_TYPE", StringType()),
    "summary_period_start": ("SUMMARY_PERIOD_START", DateType()),
    "summary_period_end": ("SUMMARY_PERIOD_END", DateType()),
    "txn_count_total": ("TXN_COUNT_TOTAL", IntegerType()),
    "txn_count_debit": ("TXN_COUNT_DEBIT", IntegerType()),
    "txn_count_credit": ("TXN_COUNT_CREDIT", IntegerType()),
    "txn_count_fee": ("TXN_COUNT_FEE", IntegerType()),
    "amt_total_debit": ("AMT_TOTAL_DEBIT", DecimalType(18, 2)),
    "amt_total_credit": ("AMT_TOTAL_CREDIT", DecimalType(18, 2)),
    "amt_total_fees": ("AMT_TOTAL_FEES", DecimalType(18, 2)),
    "amt_avg_debit": ("AMT_AVG_DEBIT", DecimalType(15, 2)),
    "amt_avg_credit": ("AMT_AVG_CREDIT", DecimalType(15, 2)),
    "amt_max_single_debit": ("AMT_MAX_SINGLE_DEBIT", DecimalType(15, 2)),
    "amt_max_single_credit": ("AMT_MAX_SINGLE_CREDIT", DecimalType(15, 2)),
    "distinct_merchants": ("DISTINCT_MERCHANTS", IntegerType()),
    "top_merchant_category": ("TOP_MERCHANT_CATEGORY", StringType()),
    "pct_atm": ("PCT_ATM", DecimalType(5, 2)),
    "pct_pos": ("PCT_POS", DecimalType(5, 2)),
    "pct_web": ("PCT_WEB", DecimalType(5, 2)),
    "pct_mobile": ("PCT_MOBILE", DecimalType(5, 2)),
    "days_since_last_txn": ("DAYS_SINCE_LAST_TXN", IntegerType()),
    "load_ts": ("LOAD_TS", TimestampType()),
}
# integer-typed columns whose CSV values look like "48.0"
FLOATY_INT_COLS = {"txn_count_debit", "txn_count_credit", "txn_count_fee"}


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("test_txn_analytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="session")
def stg_df(spark):
    df = spark.read.option("header", True).csv(str(STG_CSV))
    exprs = []
    for lower, (upper, dtype) in STG_CASTS.items():
        if lower in FLOATY_INT_COLS:
            # "48.0"-style values: go through decimal so the int cast isn't null
            exprs.append(F.col(lower).cast(DecimalType(18, 6)).cast(dtype).alias(upper))
        elif upper == "LOAD_TS":
            # "+02:00" offsets: strip before casting (session TZ is UTC)
            exprs.append(
                F.to_timestamp(
                    F.regexp_replace(F.col(lower), r"[+-]\d{2}:\d{2}$", "")
                ).alias(upper)
            )
        else:
            exprs.append(F.col(lower).cast(dtype).alias(upper))
    df = df.select(*exprs)
    # no cast may introduce new nulls
    raw_nulls = {"amt_avg_credit": 3, "top_merchant_category": 3}
    for lower, (upper, _) in STG_CASTS.items():
        expected = raw_nulls.get(lower, 0)
        got = df.where(F.col(upper).isNull()).count()
        assert got == expected, f"{upper}: {got} nulls after cast, expected {expected}"
    return df


@pytest.fixture(scope="session")
def ref_df():
    return pd.read_csv(REF_CSV)


@pytest.fixture(scope="session")
def out_df(spark, stg_df):
    return ta.transform(stg_df, AS_OF).cache()


@pytest.fixture(scope="session")
def out_pd(out_df):
    # toPandas() hits pandas-version quirks; collect() + ctor is version-proof
    return pd.DataFrame(
        [r.asDict() for r in out_df.collect()]
    ).set_index("CUSTOMER_ID").sort_index()


def test_output_schema(out_df):
    got = [(f.name, f.dataType) for f in out_df.schema]
    exp = [(f.name, f.dataType) for f in ta.OUTPUT_SCHEMA]
    assert got == exp


def test_row_count_and_keys(out_df, ref_df):
    assert out_df.count() == 500
    ids = {r["CUSTOMER_ID"] for r in out_df.select("CUSTOMER_ID").collect()}
    assert ids == set(ref_df["customer_id"])
    assert len(ids) == out_df.count()


def _num_diff(out_pd, ref, out_col, ref_col):
    a = pd.to_numeric(out_pd[out_col])
    b = pd.to_numeric(ref.set_index("customer_id").sort_index()[ref_col])
    return (a - b).abs()


PARITY_NUMERIC = [
    ("TOTAL_ACCOUNTS", "total_accounts"),
    ("ACTIVE_ACCOUNTS", "active_accounts"),
    ("TOTAL_TRANSACTIONS", "total_transactions"),
    ("TOTAL_DEBIT_AMT", "total_debit_amt"),
    ("TOTAL_CREDIT_AMT", "total_credit_amt"),
    ("NET_CASH_FLOW", "net_cash_flow"),
    ("AVG_TRANSACTION_SIZE", "avg_transaction_size"),
    ("DIGITAL_TXN_PCT", "digital_txn_pct"),
    ("FEE_INCOME", "fee_income"),
    ("INTEREST_INCOME", "interest_income"),
    ("REVENUE_CONTRIBUTION", "revenue_contribution"),
]


def test_numeric_parity(out_pd, ref_df):
    ref = ref_df.set_index("customer_id").sort_index()
    for out_col, ref_col in PARITY_NUMERIC:
        diff = (pd.to_numeric(out_pd[out_col]) - pd.to_numeric(ref[ref_col])).abs()
        assert diff.max() <= 0.01 + 1e-9, f"{out_col} max diff {diff.max()}"


def test_exact_columns(out_pd, ref_df):
    ref = ref_df.set_index("customer_id").sort_index()
    assert (out_pd["MONTHLY_SPEND_TREND"] == ref["monthly_spend_trend"]).all()
    assert (out_pd["ANOMALY_FLAG"] == ref["anomaly_flag"]).all()
    assert (out_pd["REPORTING_PERIOD"] == "2026-04").all()
    assert (out_pd["MODEL_VERSION"] == ta.MODEL_VERSION).all()
    assert (out_pd["EFFECTIVE_DATE"] == AS_OF).all()
    assert out_pd["LOAD_TS"].notna().all()


def test_anomaly_flags(out_pd, ref_df, out_df):
    ref = ref_df.set_index("customer_id").sort_index()
    y = out_pd[out_pd["ANOMALY_FLAG"] == "Y"]
    assert len(y) == 4
    assert set(y.index) == set(ref[ref["anomaly_flag"] == "Y"].index)
    median = ta.qntldef5_quantile(out_df, "TOTAL_DEBIT_AMT", 0.5)
    q1 = ta.qntldef5_quantile(out_df, "TOTAL_DEBIT_AMT", 0.25)
    q3 = ta.qntldef5_quantile(out_df, "TOTAL_DEBIT_AMT", 0.75)
    assert median == Decimal("8771.25")
    assert q3 - q1 == Decimal("8051.765")


def test_spend_percentile(out_pd, ref_df):
    ref = ref_df.set_index("customer_id").sort_index()
    # contract = PROC RANK GROUPS=100: floor(avg_rank * 100 / (n + 1))
    avg_rank = ref["total_debit_amt"].rank(method="average")
    expected = (avg_rank * 100 / (len(ref) + 1)).apply(math.floor)
    got = pd.to_numeric(out_pd["SPEND_PERCENTILE"])
    assert (got == expected).all(), (got - expected).abs().max()
    # known deviation: the reference CSV used rank/n*100 instead
    ref_diff = (got - pd.to_numeric(ref["spend_percentile"])).abs()
    assert ref_diff.max() <= 1.0


def test_top_spend_category(out_pd, ref_df, stg_df):
    stg = pd.DataFrame([r.asDict() for r in stg_df.collect()])
    expected = (
        stg.dropna(subset=["TOP_MERCHANT_CATEGORY"])
        .groupby("CUSTOMER_ID")["TOP_MERCHANT_CATEGORY"]
        .max()
    )
    for cust_id, exp in expected.items():
        assert out_pd.loc[cust_id, "TOP_SPEND_CATEGORY"] == exp
    ref = ref_df.set_index("customer_id").sort_index()
    single_val = stg.dropna(subset=["TOP_MERCHANT_CATEGORY"]).groupby("CUSTOMER_ID")[
        "TOP_MERCHANT_CATEGORY"
    ].nunique()
    single_ids = single_val[single_val == 1].index
    agree = (
        out_pd.loc[single_ids, "TOP_SPEND_CATEGORY"]
        == ref.loc[single_ids, "top_spend_category"]
    )
    assert agree.all()


def test_parity_summary(out_pd, ref_df, stg_df, capsys):
    ref = ref_df.set_index("customer_id").sort_index()
    lines = []
    for out_col, ref_col in PARITY_NUMERIC:
        diff = (pd.to_numeric(out_pd[out_col]) - pd.to_numeric(ref[ref_col])).abs()
        lines.append(f"{out_col}: max_abs_diff={diff.max():.6f}")
    for out_col, ref_col in [
        ("MONTHLY_SPEND_TREND", "monthly_spend_trend"),
        ("ANOMALY_FLAG", "anomaly_flag"),
        ("TOP_SPEND_CATEGORY", "top_spend_category"),
        ("REPORTING_PERIOD", "reporting_period"),
        ("MODEL_VERSION", "model_version"),
        ("EFFECTIVE_DATE", "effective_date"),
    ]:
        agree = (out_pd[out_col].astype(str) == ref[ref_col].astype(str)).mean()
        lines.append(f"{out_col}: agreement={agree:.4f}")
    pct_diff = (
        pd.to_numeric(out_pd["SPEND_PERCENTILE"]) - pd.to_numeric(ref["spend_percentile"])
    ).abs()
    lines.append(
        f"SPEND_PERCENTILE: max_abs_diff={pct_diff.max():.6f} "
        f"mean_abs_diff={pct_diff.mean():.6f} (known deviation, GROUPS=100 vs rank/n)"
    )
    lines.append("LOAD_TS: not-null only (audit timestamp)")
    summary = "\n".join(lines)
    print("\n=== PARITY SUMMARY ===\n" + summary)
    Path(PARITY_TXT).write_text(summary + "\n")
    assert True


# ------------------------- unit tests on small frames -------------------------


def test_proc_rank_groups_ties_and_null(spark):
    df = spark.createDataFrame(
        [(10.0,), (20.0,), (20.0,), (30.0,), (None,)], ["v"]
    )
    out = ta.proc_rank_groups(df, "v", 4, "r").orderBy(F.col("v").asc_nulls_last())
    got = [r["r"] for r in out.collect()]
    assert got == [0, 2, 2, 3, None]


def test_qntldef5_quantile_branches(spark):
    df4 = spark.createDataFrame([(float(i),) for i in [1, 2, 3, 4]], ["v"])
    df5 = spark.createDataFrame([(float(i),) for i in [1, 2, 3, 4, 5]], ["v"])
    empty = spark.createDataFrame([], "v double")
    assert ta.qntldef5_quantile(df4, "v", 0.5) == Decimal("2.5")
    assert ta.qntldef5_quantile(df5, "v", 0.5) == Decimal("3")
    assert ta.qntldef5_quantile(df4, "v", 0.25) == Decimal("1.5")
    assert ta.qntldef5_quantile(df5, "v", 0.25) == Decimal("2")
    assert ta.qntldef5_quantile(empty, "v", 0.5) is None


def _tiny_stg(spark, rows):
    cols = [
        "CUSTOMER_ID", "ACCOUNT_ID", "DAYS_SINCE_LAST_TXN", "TXN_COUNT_TOTAL",
        "AMT_TOTAL_DEBIT", "AMT_TOTAL_CREDIT", "AMT_TOTAL_FEES",
        "TOP_MERCHANT_CATEGORY", "PCT_WEB", "PCT_MOBILE",
    ]
    from decimal import Decimal as D
    data = [
        (
            r[0], r[1], r[2], r[3],
            D(str(r[4])), D(str(r[5])), D(str(r[6])),
            r[7], D(str(r[8])), D(str(r[9])),
        )
        for r in rows
    ]
    schema = (
        "CUSTOMER_ID bigint, ACCOUNT_ID bigint, DAYS_SINCE_LAST_TXN int, "
        "TXN_COUNT_TOTAL int, AMT_TOTAL_DEBIT decimal(18,2), "
        "AMT_TOTAL_CREDIT decimal(18,2), AMT_TOTAL_FEES decimal(18,2), "
        "TOP_MERCHANT_CATEGORY string, PCT_WEB decimal(5,2), PCT_MOBILE decimal(5,2)"
    )
    return spark.createDataFrame(data, schema)


def test_zero_txn_count_gives_zero_avg_and_digital(spark):
    stg = _tiny_stg(spark, [(1, 1, 5, 0, 0, 0, 0, "X", 10, 10)])
    out = ta.transform(stg, AS_OF).collect()[0]
    assert out["AVG_TRANSACTION_SIZE"] == 0
    assert out["DIGITAL_TXN_PCT"] == 0


def test_anomaly_exact_decimal_large_totals(spark):
    # regression: float quantiles collapsed IQR to 0 at 1e16 totals, hiding
    # anomalies. Cents must survive exact-Decimal quantile math.
    base = Decimal("9000000000000000")
    cents = ["00", "01", "02", "03", "04", "05", "20"]
    stg = _tiny_stg(
        spark,
        [
            (i + 1, i + 1, 5, 10, f"{base}.{c}", "0", "0", "X", "0", "0")
            for i, c in enumerate(cents)
        ],
    )
    out = ta.transform(stg, AS_OF).cache()
    q1 = ta.qntldef5_quantile(out, "TOTAL_DEBIT_AMT", 0.25)
    q3 = ta.qntldef5_quantile(out, "TOTAL_DEBIT_AMT", 0.75)
    assert q1 == Decimal("9000000000000000.01")
    assert q3 == Decimal("9000000000000000.05")
    assert q3 - q1 == Decimal("0.04")
    flags = {r["CUSTOMER_ID"]: r["ANOMALY_FLAG"] for r in out.collect()}
    assert flags[7] == "Y"
    assert all(v == "N" for k, v in flags.items() if k != 7)


def test_anomaly_all_n_when_iqr_zero(spark):
    stg = _tiny_stg(
        spark,
        [
            (c, c, 5, 10, 100, 50, 1, "X", 10, 10)
            for c in range(1, 8)
        ],
    )
    out = ta.transform(stg, AS_OF)
    assert {r["ANOMALY_FLAG"] for r in out.collect()} == {"N"}


# ------------------------------- main() tests -------------------------------


def _fake_pipeline_utils(monkeypatch, stg_df, calls, validate_raises=False, env="dev"):
    pu = types.ModuleType("synapse.spark.common.pipeline_utils")

    class ValidationError(Exception):
        pass

    class RunParams:
        def __init__(self):
            self.as_of_date = AS_OF
            self.env = env
            self.lookback_months = 12
            self.risk_score_threshold = 0.5

    pu.ValidationError = ValidationError
    pu.get_run_params = lambda argv: RunParams()
    pu.read_table = lambda spark, schema, table: stg_df

    def log_step(spark, job_name, step_name, status, row_count=None, msg=None):
        calls.setdefault("log", []).append(
            (job_name, step_name, status, row_count, msg)
        )

    def validate_table(df, key_cols, not_null_cols, min_rows=1):
        calls["validate"] = (key_cols, not_null_cols, min_rows)
        # enforce the row count like the real helper
        n = df.count()
        if n < min_rows or validate_raises:
            raise ValidationError(f"row count {n} < min_rows {min_rows}")

    def write_table(df, schema, table, mode="overwrite", pre_delete_where=None):
        calls["write"] = (schema, table, mode, pre_delete_where)

    pu.log_step = log_step
    pu.validate_table = validate_table
    pu.write_table = write_table

    for name in ["synapse", "synapse.spark", "synapse.spark.common"]:
        monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "synapse.spark.common.pipeline_utils", pu)
    import synapse.spark.common as common_pkg
    monkeypatch.setattr(common_pkg, "pipeline_utils", pu, raising=False)
    return pu


def test_main_happy_path(spark, stg_df, monkeypatch):
    calls = {}
    _fake_pipeline_utils(monkeypatch, stg_df, calls)  # env="dev"
    rc = ta.main(["--as-of-date", "2026-04-10", "--env", "dev"])
    assert rc == 0
    assert calls["write"] == (
        "dp", "TRANSACTION_ANALYTICS", "append", "REPORTING_PERIOD = '2026-04'"
    )
    # enforcing fake: 500 rows passes in dev because min_rows relaxes to 1
    assert calls["validate"] == (
        ta.VALIDATION_KEY_COLS, ta.VALIDATION_NOT_NULL_COLS, 1
    )
    statuses = [c[2] for c in calls["log"]]
    assert statuses[0] == "START" and statuses[-1] == "SUCCESS"


@pytest.mark.parametrize(
    "env,expected",
    [
        ("dev", 1), ("uat", 1000), ("prod", 1000),
        ("DEV", 1), ("UAT", 1000), ("PROD", 1000), ("Prod", 1000),
    ],
)
def test_validation_min_rows(env, expected):
    assert ta.validation_min_rows(env) == expected


@pytest.mark.parametrize("env", ["prod", "PROD"])
def test_main_prod_enforces_min_rows(spark, stg_df, monkeypatch, env):
    calls = {}
    pu = _fake_pipeline_utils(monkeypatch, stg_df, calls, env=env)
    with pytest.raises(pu.ValidationError, match="min_rows 1000"):
        ta.main(["--as-of-date", "2026-04-10", "--env", env])
    assert calls["validate"][2] == 1000
    assert "write" not in calls
    assert any(c[2] == "FAILED" for c in calls["log"])


def test_main_validation_failure(spark, stg_df, monkeypatch):
    calls = {}
    pu = _fake_pipeline_utils(monkeypatch, stg_df, calls, validate_raises=True)
    with pytest.raises(pu.ValidationError):
        ta.main(["--as-of-date", "2026-04-10", "--env", "dev"])
    assert "write" not in calls
    assert any(c[2] == "FAILED" for c in calls["log"])
