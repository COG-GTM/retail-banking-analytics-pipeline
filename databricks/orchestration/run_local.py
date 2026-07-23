"""Local end-to-end runner (no Databricks cluster required).

    python -m orchestration.run_local --catalog spark_catalog --min-rows 1

Creates the catalog/schemas/tables, lands the sample CSVs, then runs the full
pipeline. On Databricks the Workflow in ``databricks/resources`` runs the same
step functions instead; there the source tables are populated upstream so the
``--load-sample-data`` step is skipped.
"""
from __future__ import annotations

import argparse

from common import ddl
from common.config import load_config
from common.spark_utils import get_spark
from orchestration.load_sample_data import load_sample_sources
from orchestration.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail-banking pipeline locally.")
    parser.add_argument("--catalog", default="spark_catalog")
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--run-date", default=None, help="YYYY-MM-DD (defaults to today)")
    parser.add_argument("--skip-sample-data", action="store_true")
    args = parser.parse_args()

    spark = get_spark()
    overrides = {"catalog": args.catalog}
    if args.run_date:
        from datetime import datetime

        overrides["run_date"] = datetime.strptime(args.run_date, "%Y-%m-%d").date()
    config = load_config(spark=spark, **overrides)

    ddl.create_all(spark, config)
    if not args.skip_sample_data:
        load_sample_sources(spark, config)

    counts = run_pipeline(spark, config, min_rows=args.min_rows)
    print("Pipeline complete. Row counts:")
    for name, count in counts.items():
        print(f"  {name}: {count}")


if __name__ == "__main__":
    main()
