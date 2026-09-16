import sys
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA = REPO_ROOT / "data"
sys.path.insert(0, str(REPO_ROOT / "databricks" / "src"))

RUN_DATE = date(2026, 4, 10)  # date the sample CSVs were generated

SOURCE_CSVS = {
    "customers": "01_source_tables/customers.csv",
    "addresses": "01_source_tables/addresses.csv",
    "accounts": "01_source_tables/accounts.csv",
    "customer_bureau_scores": "01_source_tables/customer_bureau_scores.csv",
    "transactions": "01_source_tables/transactions.csv",
    "transaction_types": "01_source_tables/transaction_types.csv",
}
DATE_COLS = {
    "customers": ["date_of_birth", "customer_since"],
    "addresses": ["effective_date", "expiration_date"],
    "accounts": ["open_date", "close_date"],
    "customer_bureau_scores": ["report_date"],
    "transactions": ["transaction_date"],
    "transaction_types": ["effective_date", "expiration_date"],
}
TS_COLS = {
    "customers": ["created_ts", "updated_ts"],
    "addresses": ["created_ts", "updated_ts"],
    "accounts": ["created_ts", "updated_ts"],
    "transactions": ["transaction_ts", "created_ts"],
}


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]")
         .appName("retail-banking-tests")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.host", "127.0.0.1")
         .getOrCreate())
    yield s
    s.stop()


def _load_source(spark, name: str):
    import pyspark.sql.functions as F
    df = (spark.read.option("header", "true").option("inferSchema", "true")
          .csv(str(DATA / SOURCE_CSVS[name])))
    for c in DATE_COLS.get(name, []):
        df = df.withColumn(c, F.to_date(c))
    for c in TS_COLS.get(name, []):
        df = df.withColumn(c, F.to_timestamp(c))
    return df


@pytest.fixture(scope="session")
def sources(spark):
    return {name: _load_source(spark, name) for name in SOURCE_CSVS}


@pytest.fixture(scope="session")
def silver_expected(spark):
    out = {}
    for t in ("stg_customer_360", "stg_txn_summary", "stg_risk_factors"):
        out[t] = (spark.read.option("header", "true")
                  .option("inferSchema", "true")
                  .csv(str(DATA / "02_bteq_staging" / f"{t}.csv")))
    return out


@pytest.fixture(scope="session")
def gold_expected(spark):
    out = {}
    for t in ("customer_segments", "transaction_analytics",
              "customer_risk_scores", "customer_master_profile"):
        out[t] = (spark.read.option("header", "true")
                  .option("inferSchema", "true")
                  .csv(str(DATA / "03_sas_data_products" / f"{t}.csv")))
    return out


@pytest.fixture(scope="session")
def silver_actual(sources):
    from retail_banking.silver import (
        build_stg_customer_360,
        build_stg_risk_factors,
        build_stg_txn_summary,
    )
    c360 = build_stg_customer_360(
        sources["customers"], sources["addresses"], sources["accounts"],
        RUN_DATE).cache()
    txn = build_stg_txn_summary(
        sources["transactions"], sources["accounts"],
        sources["transaction_types"], RUN_DATE, 12).cache()
    risk = build_stg_risk_factors(
        sources["customers"], sources["accounts"], sources["transactions"],
        sources["transaction_types"], sources["customer_bureau_scores"],
        RUN_DATE).cache()
    c360.count(); txn.count(); risk.count()
    return {"stg_customer_360": c360, "stg_txn_summary": txn,
            "stg_risk_factors": risk}


@pytest.fixture(scope="session")
def gold_actual(silver_actual):
    from retail_banking.gold import (
        build_customer_master_profile,
        build_customer_risk_scores,
        build_customer_segments,
        build_transaction_analytics,
    )
    seg = build_customer_segments(
        silver_actual["stg_customer_360"], RUN_DATE).cache()
    txn = build_transaction_analytics(
        silver_actual["stg_txn_summary"], RUN_DATE).cache()
    risk = build_customer_risk_scores(
        silver_actual["stg_risk_factors"],
        silver_actual["stg_customer_360"], RUN_DATE).cache()
    mp = build_customer_master_profile(
        silver_actual["stg_customer_360"], seg, txn, risk, RUN_DATE).cache()
    seg.count(); txn.count(); risk.count(); mp.count()
    return {"customer_segments": seg, "transaction_analytics": txn,
            "customer_risk_scores": risk, "customer_master_profile": mp}
