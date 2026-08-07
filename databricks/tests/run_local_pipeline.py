"""Run the whole Databricks pipeline on local Spark and diff it against the reference.

Every notebook keeps its transformations in plain functions guarded by
``if in_databricks()``, so the same code that runs on the Workflow can be
executed here against ``data/01_source_tables/*.csv`` with no cluster.

The output is compared with the DuckDB/scikit-learn reference exports in
``data/02_bteq_staging`` and ``data/03_sas_data_products``: row counts for every
layer, plus segment distribution, risk-tier distribution and composite-score
statistics.

The committed extracts are a point-in-time snapshot while several transforms are
relative to ``current_date()`` (12-month lookback, 30/90-day balance windows,
tenure, age), so for a like-for-like comparison regenerate the reference first
and point both sides at it::

    uv run export_data.py --customers 500
    python databricks/tests/run_local_pipeline.py

    python databricks/tests/run_local_pipeline.py --data-root DIR [--output-dir DIR]
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
DATABRICKS_ROOT = REPO_ROOT / "databricks"
sys.path.insert(0, str(DATABRICKS_ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F

from shared.schemas import GOLD_SCHEMAS, SOURCE_FILES  # noqa: E402

NOTEBOOKS = {
    "bronze": "notebooks/bronze/01_load_source_tables.py",
    "customer_360": "notebooks/silver/01_stg_customer_360.py",
    "txn_summary": "notebooks/silver/02_stg_txn_summary.py",
    "risk_factors": "notebooks/silver/03_stg_risk_factors.py",
    "segments": "notebooks/gold/01_customer_segments.py",
    "txn_analytics": "notebooks/gold/02_txn_analytics.py",
    "risk_scoring": "notebooks/gold/03_risk_scoring.py",
    "data_products": "notebooks/gold/04_data_products.py",
}


def load_notebook(name: str) -> ModuleType:
    path = DATABRICKS_ROOT / NOTEBOOKS[name]
    spec = importlib.util.spec_from_file_location(f"nb_{name}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("retail-banking-local-validation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def read_reference(spark: SparkSession, data_root: Path, relative_path: str) -> DataFrame | None:
    path = data_root / relative_path
    if not path.exists():
        return None
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(path))
    return df.select([F.col(c).alias(c.upper()) for c in df.columns])


def run_pipeline(spark: SparkSession, data_root: Path = REPO_ROOT) -> dict[str, DataFrame]:
    bronze_nb = load_notebook("bronze")
    source_dir = data_root / "data" / "01_source_tables"
    bronze = {
        table: bronze_nb.read_source_csv(spark, str(source_dir / f"{file_name}.csv"), table)
        for file_name, table in SOURCE_FILES.items()
    }
    for table, df in bronze.items():
        df.cache()
        print(f"  bronze {table:<24} {df.count():>8,} rows")

    customer_360 = load_notebook("customer_360").build_stg_customer_360(
        bronze["CUSTOMERS"], bronze["ACCOUNTS"], bronze["ADDRESSES"]
    ).cache()
    txn_summary = load_notebook("txn_summary").build_stg_txn_summary(
        bronze["TRANSACTIONS"], bronze["ACCOUNTS"], bronze["TRANSACTION_TYPES"]
    ).cache()
    risk_factors = load_notebook("risk_factors").build_stg_risk_factors(
        bronze["CUSTOMERS"],
        bronze["ACCOUNTS"],
        bronze["TRANSACTIONS"],
        bronze["TRANSACTION_TYPES"],
        bronze["CUSTOMER_BUREAU_SCORES"],
    ).cache()

    segments = load_notebook("segments").build_customer_segments(customer_360).cache()
    period = spark.sql("SELECT date_format(current_date(), 'yyyy-MM')").collect()[0][0]
    txn_analytics = load_notebook("txn_analytics").build_txn_analytics(txn_summary, period).cache()
    risk_scores = (
        load_notebook("risk_scoring")
        .build_customer_risk_scores(risk_factors, customer_360)
        .cache()
    )
    master_profile = (
        load_notebook("data_products")
        .build_master_profile(customer_360, segments, txn_analytics, risk_scores)
        .cache()
    )

    return {
        "STG_CUSTOMER_360": customer_360,
        "STG_TXN_SUMMARY": txn_summary,
        "STG_RISK_FACTORS": risk_factors,
        "CUSTOMER_SEGMENTS": segments,
        "TRANSACTION_ANALYTICS": txn_analytics,
        "CUSTOMER_RISK_SCORES": risk_scores,
        "CUSTOMER_MASTER_PROFILE": master_profile,
    }


REFERENCE_FILES = {
    "STG_CUSTOMER_360": "data/02_bteq_staging/stg_customer_360.csv",
    "STG_TXN_SUMMARY": "data/02_bteq_staging/stg_txn_summary.csv",
    "STG_RISK_FACTORS": "data/02_bteq_staging/stg_risk_factors.csv",
    "CUSTOMER_SEGMENTS": "data/03_sas_data_products/customer_segments.csv",
    "TRANSACTION_ANALYTICS": "data/03_sas_data_products/transaction_analytics.csv",
    "CUSTOMER_RISK_SCORES": "data/03_sas_data_products/customer_risk_scores.csv",
    "CUSTOMER_MASTER_PROFILE": "data/03_sas_data_products/customer_master_profile.csv",
}


def compare_row_counts(
    spark: SparkSession, data_root: Path, tables: dict[str, DataFrame]
) -> list[str]:
    print("\nRow counts (PySpark vs DuckDB reference)")
    print(f"  {'table':<26}{'pyspark':>10}{'reference':>12}{'delta':>8}")
    problems = []
    for name, df in tables.items():
        actual = df.count()
        reference = read_reference(spark, data_root, REFERENCE_FILES[name])
        expected = None if reference is None else reference.count()
        delta = "" if expected is None else f"{actual - expected:+d}"
        print(f"  {name:<26}{actual:>10,}{(expected or 0):>12,}{delta:>8}")
        if expected is not None and actual != expected:
            problems.append(f"{name}: {actual} rows, reference has {expected}")
    return problems


def check_gold_schemas(tables: dict[str, DataFrame]) -> list[str]:
    print("\nGold schema contract (ddl/02_data_product_tables.sql)")
    problems = []
    for name, spec in GOLD_SCHEMAS.items():
        actual = [(f.name, f.dataType.simpleString()) for f in tables[name].schema.fields]
        if actual == spec:
            print(f"  {name:<26} OK ({len(spec)} columns)")
        else:
            problems.append(f"{name}: schema mismatch\n    got      {actual}\n    expected {spec}")
    return problems


def report_distributions(
    spark: SparkSession, data_root: Path, tables: dict[str, DataFrame]
) -> None:
    print("\nSegment distribution")
    tables["CUSTOMER_SEGMENTS"].groupBy("SEGMENT_NAME").agg(
        F.count(F.lit(1)).alias("N"), F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV")
    ).orderBy(F.col("N").desc()).show(truncate=False)

    reference = read_reference(spark, data_root, REFERENCE_FILES["CUSTOMER_SEGMENTS"])
    if reference is not None:
        print("Segment distribution — reference")
        reference.groupBy("SEGMENT_NAME").agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV"),
        ).orderBy(F.col("N").desc()).show(truncate=False)

    print("Risk tier distribution")
    tables["CUSTOMER_RISK_SCORES"].groupBy("RISK_TIER").agg(
        F.count(F.lit(1)).alias("N"), F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE")
    ).orderBy(F.col("AVG_SCORE").desc()).show(truncate=False)

    reference = read_reference(spark, data_root, REFERENCE_FILES["CUSTOMER_RISK_SCORES"])
    if reference is not None:
        print("Risk tier distribution — reference")
        reference.groupBy("RISK_TIER").agg(
            F.count(F.lit(1)).alias("N"),
            F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE"),
        ).orderBy(F.col("AVG_SCORE").desc()).show(truncate=False)

    stats = ["COMPOSITE_RISK_SCORE", "PROBABILITY_OF_DEFAULT"]
    print("Composite score statistics")
    tables["CUSTOMER_RISK_SCORES"].select(stats).summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
    ).show(truncate=False)
    if reference is not None:
        print("Composite score statistics — reference")
        reference.select(stats).summary(
            "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
        ).show(truncate=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=REPO_ROOT,
        help="directory containing data/01_source_tables and the reference exports",
    )
    parser.add_argument("--output-dir", help="write the produced tables as CSV here")
    args = parser.parse_args()

    spark = build_session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = run_pipeline(spark, args.data_root)
        problems = compare_row_counts(spark, args.data_root, tables) + check_gold_schemas(tables)
        report_distributions(spark, args.data_root, tables)

        if args.output_dir:
            for name, df in tables.items():
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
                    str(Path(args.output_dir) / name.lower())
                )

        if problems:
            print("\nDifferences vs the reference implementation:")
            for problem in problems:
                print(f"  - {problem}")
            return 1
        print("\nAll layers match the DuckDB reference row counts and the DDL contract.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
