"""Run the whole pipeline end-to-end against a local Spark session.

Executes every job in :data:`orchestration.pipeline.STEPS` in dependency order,
sharing the Ticket-3 logging/validation, so the full bronze->silver->gold flow
(culminating in ``data_products.customer_master_profile``) can be exercised in
tests/CI without Databricks.  Mirrors the legacy ``run_full_pipeline.sh``.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from common.config import Config, get_config
from common.spark import get_spark
from orchestration.pipeline import topological_order


def run_pipeline(
    spark: Optional[SparkSession] = None,
    cfg: Optional[Config] = None,
) -> dict[str, object]:
    """Run all pipeline steps in order; return each step's result by name."""
    spark = spark or get_spark()
    cfg = cfg or get_config()

    results: dict[str, object] = {}
    for step in topological_order():
        results[step.name] = step.callable()(spark, cfg)
    return results


def main() -> None:  # pragma: no cover - CLI entry point
    run_pipeline()


if __name__ == "__main__":  # pragma: no cover
    main()
