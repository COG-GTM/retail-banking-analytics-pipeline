"""CLI entry point that runs the full pipeline DAG locally.

Ports ``orchestration/run_full_pipeline.sh`` (``--dry-run`` supported). Uses
``LocalDataIO`` (CSV in / Parquet lake out); production runs swap in a JDBC/Delta
IO backend without touching the jobs.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from common.config import PipelineConfig
from common.io import LocalDataIO
from common.spark import build_spark
from orchestration.pipeline import PipelineError, dry_run_plan, run_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the retail-banking analytics pipeline")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None, help="YYYY-MM-DD (default: config/today)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.dry_run:
        for step in dry_run_plan():
            print(step)
        return 0

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark("retail_banking_analytics_pipeline")
    io = LocalDataIO(spark, config, args.source_dir, args.lake_dir)
    try:
        run = run_pipeline(spark, io, config)
    except PipelineError as exc:
        print(f"PIPELINE FAILED: {exc}")
        return 2
    print(f"PIPELINE SUCCESS: {len(run.results)} tasks, run_id={run.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
