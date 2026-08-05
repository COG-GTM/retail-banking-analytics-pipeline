"""End-to-end PySpark pipeline orchestrator.

Replaces orchestration/run_full_pipeline.sh: runs the three BTEQ-equivalent
staging jobs followed by the four SAS-equivalent analytics jobs, writing each
output as CSV under --output-dir.

Usage:
    python run_pipeline.py [--run-date 2026-04-10] [--output-dir ../output]
"""
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

from common import DEFAULT_RUN_DATE, get_spark, load_source_tables
from customer_segments import build_customer_segments
from master_profile import build_customer_master_profile
from risk_scoring import build_customer_risk_scores
from stg_customer_360 import build_stg_customer_360
from stg_risk_factors import build_stg_risk_factors
from stg_txn_summary import build_stg_txn_summary
from txn_analytics import build_transaction_analytics


def write_csv(df, out_dir: Path, name: str) -> int:
    """Write a DataFrame to a single CSV file and return its row count."""
    pdf = df.toPandas()
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(out_dir / f"{name}.csv", index=False)
    return len(pdf)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", type=dt.date.fromisoformat, default=DEFAULT_RUN_DATE)
    parser.add_argument(
        "--output-dir", type=Path, default=Path(__file__).resolve().parent.parent / "output"
    )
    args = parser.parse_args()

    spark = get_spark("retail-banking-full-pipeline")
    load_source_tables(spark)

    # Phase 1: BTEQ-equivalent staging (Bronze -> Silver)
    stg_c360 = build_stg_customer_360(spark, args.run_date).cache()
    stg_txn = build_stg_txn_summary(spark, args.run_date).cache()
    stg_risk = build_stg_risk_factors(spark, args.run_date).cache()

    # Phase 2: SAS-equivalent analytics (Silver -> Gold)
    segments = build_customer_segments(spark, stg_c360, args.run_date).cache()
    txn_analytics = build_transaction_analytics(spark, stg_txn, args.run_date).cache()
    risk_scores = build_customer_risk_scores(spark, stg_risk, stg_c360, args.run_date).cache()
    master = build_customer_master_profile(
        spark, stg_c360, segments, txn_analytics, risk_scores, args.run_date
    )

    staging_dir = args.output_dir / "02_bteq_staging"
    products_dir = args.output_dir / "03_sas_data_products"
    for name, df, out in [
        ("stg_customer_360", stg_c360, staging_dir),
        ("stg_txn_summary", stg_txn, staging_dir),
        ("stg_risk_factors", stg_risk, staging_dir),
        ("customer_segments", segments, products_dir),
        ("transaction_analytics", txn_analytics, products_dir),
        ("customer_risk_scores", risk_scores, products_dir),
        ("customer_master_profile", master, products_dir),
    ]:
        rows = write_csv(df, out, name)
        print(f"  {name}: {rows:,} rows -> {out / (name + '.csv')}")

    spark.stop()


if __name__ == "__main__":
    main()
