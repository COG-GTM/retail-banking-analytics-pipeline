"""Single-task entry point used by the Databricks Workflow (databricks.yml).

Each Asset Bundle task invokes ``run_job.py --job <name>`` which dispatches to the
matching step in :data:`orchestration.pipeline.STEPS`.  The same script runs a
job locally for ad-hoc debugging.
"""

from __future__ import annotations

import argparse

from common.config import get_config
from common.spark import get_spark
from orchestration.pipeline import STEPS_BY_NAME, get_step


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single pipeline job.")
    parser.add_argument(
        "--job",
        required=True,
        choices=sorted(STEPS_BY_NAME),
        help="Name of the pipeline step to run.",
    )
    args = parser.parse_args()

    step = get_step(args.job)
    if step is None:  # pragma: no cover - guarded by argparse choices
        raise SystemExit(f"Unknown job: {args.job}")

    spark = get_spark()
    cfg = get_config()
    step.callable()(spark, cfg)


if __name__ == "__main__":  # pragma: no cover
    main()
