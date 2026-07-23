"""Shared helper for the parallel-migration stub jobs.

Jobs 1-9 land in separate parallel PRs.  Until they are on the default branch,
Ticket 10 ships minimal, contract-compatible stubs so the end-to-end
orchestration test can run.  Each stub loads the corresponding committed seed CSV
under ``data/`` into its Delta table via :class:`Config`, mirroring the
``run(spark, cfg) -> DataFrame`` job contract.  The owning ticket's real
transform supersedes the stub at merge.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.audit import init_audit, log_step
from common.config import Config
from common.spark import read_delta, write_delta
from common.validation import validate_table


def load_seed(
    spark: SparkSession,
    cfg: Config,
    *,
    job_name: str,
    csv_relpath: str,
    schema: str,
    name: str,
) -> DataFrame:
    """Load a seed CSV into a Delta table and return the written DataFrame."""
    run_id = str(uuid.uuid4())
    init_audit(spark, cfg)
    log_step(spark, cfg, run_id, job_name, "load", "START",
             message=f"[stub] Seeding {schema}.{name} from {csv_relpath}")

    csv_path = str(Path(cfg.data_dir) / csv_relpath)
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )
    if "customer_id" in df.columns:
        df = df.withColumn("customer_id", F.col("customer_id").cast("long"))

    validate_table(df, min_rows=1)
    write_delta(df, cfg, schema, name, mode="overwrite")

    result = read_delta(spark, cfg, schema, name)
    log_step(spark, cfg, run_id, job_name, "load", "SUCCESS",
             row_count=result.count(),
             message=f"[stub] Seeded {schema}.{name}")
    return result
