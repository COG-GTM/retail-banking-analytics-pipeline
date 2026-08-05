"""Framework-agnostic DAG driver.

Reproduces ``orchestration/run_full_pipeline.sh`` -> ``bteq/run_bteq_pipeline.sh`` ->
``sas/run_sas_pipeline.sh``:

* the BTEQ phase runs first and, if it fails, the SAS phase never runs
  (``set -euo pipefail`` plus the explicit ``ABORT`` branch in the master script);
* inside a phase the jobs run in the listed order and a failure aborts the rest of the phase;
* a job that returns the SAS "warnings" return code (1) is tolerated and the phase continues;
* every job downstream of a failure is reported as ``SKIPPED`` rather than silently dropped;
* phase 3 re-counts the four data product tables, as the master script's post-run BTEQ block does.

The driver knows nothing about Airflow/Databricks; :func:`iter_tasks` exposes the same graph so a
scheduler wrapper can be generated from it (see ``orchestration/airflow_dag.py``).
"""

from __future__ import annotations

import importlib
import json
import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import DataIO
from common.job import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    JobResult,
    JobRunner,
    build_arg_parser,
    config_from_args,
    io_from_args,
)
from common.spark import build_spark_session
from common.validation import ValidationFailedError

LOGGER = logging.getLogger(__name__)

PHASE_BTEQ = "bteq"
PHASE_SAS = "sas"


@dataclass(frozen=True)
class JobNode:
    """One node of the legacy dependency graph."""

    name: str
    phase: str
    module: str
    target: str
    legacy_source: str
    depends_on: tuple[str, ...] = ()

    def runner(self) -> JobRunner:
        return importlib.import_module(self.module).run


#: The legacy graph, in legacy execution order.
PIPELINE: tuple[JobNode, ...] = (
    JobNode(
        name="01_stg_customer_360",
        phase=PHASE_BTEQ,
        module="jobs.stg_customer_360",
        target="ETL_STAGING_DB.STG_CUSTOMER_360",
        legacy_source="bteq/01_stg_customer_360.bteq",
    ),
    JobNode(
        name="02_stg_txn_summary",
        phase=PHASE_BTEQ,
        module="jobs.stg_txn_summary",
        target="ETL_STAGING_DB.STG_TXN_SUMMARY",
        legacy_source="bteq/02_stg_txn_summary.bteq",
        depends_on=("01_stg_customer_360",),
    ),
    JobNode(
        name="03_stg_risk_factors",
        phase=PHASE_BTEQ,
        module="jobs.stg_risk_factors",
        target="ETL_STAGING_DB.STG_RISK_FACTORS",
        legacy_source="bteq/03_stg_risk_factors.bteq",
        depends_on=("02_stg_txn_summary",),
    ),
    JobNode(
        name="01_sas_customer_segments",
        phase=PHASE_SAS,
        module="jobs.sas_customer_segments",
        target="DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS",
        legacy_source="sas/01_sas_customer_segments.sas",
        depends_on=("01_stg_customer_360",),
    ),
    JobNode(
        name="02_sas_txn_analytics",
        phase=PHASE_SAS,
        module="jobs.sas_txn_analytics",
        target="DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS",
        legacy_source="sas/02_sas_txn_analytics.sas",
        depends_on=("01_sas_customer_segments", "02_stg_txn_summary"),
    ),
    JobNode(
        name="03_sas_risk_scoring",
        phase=PHASE_SAS,
        module="jobs.sas_risk_scoring",
        target="DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES",
        legacy_source="sas/03_sas_risk_scoring.sas",
        depends_on=("02_sas_txn_analytics", "03_stg_risk_factors"),
    ),
    JobNode(
        name="04_sas_data_products",
        phase=PHASE_SAS,
        module="jobs.sas_data_products",
        target="DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE",
        legacy_source="sas/04_sas_data_products.sas",
        depends_on=("01_sas_customer_segments", "02_sas_txn_analytics", "03_sas_risk_scoring"),
    ),
)

#: Tables the master script counts in its post-run validation block.
POST_RUN_TABLES: tuple[tuple[str, str], ...] = (
    ("DATA_PRODUCTS_DB", "CUSTOMER_SEGMENTS"),
    ("DATA_PRODUCTS_DB", "TRANSACTION_ANALYTICS"),
    ("DATA_PRODUCTS_DB", "CUSTOMER_RISK_SCORES"),
    ("DATA_PRODUCTS_DB", "CUSTOMER_MASTER_PROFILE"),
)


@dataclass
class PipelineRun:
    """Outcome of one end-to-end driver invocation."""

    run_timestamp: str
    run_date: str
    start_ts: datetime = field(default_factory=datetime.now)
    end_ts: datetime | None = None
    results: list[JobResult] = field(default_factory=list)
    post_run_counts: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False

    @property
    def elapsed_seconds(self) -> float:
        return ((self.end_ts or datetime.now()) - self.start_ts).total_seconds()

    @property
    def failed(self) -> list[JobResult]:
        return [result for result in self.results if result.status == STATUS_FAILED]

    @property
    def return_code(self) -> int:
        return max((result.return_code for result in self.results), default=0)

    def result_for(self, job_name: str) -> JobResult | None:
        return next((result for result in self.results if result.job_name == job_name), None)

    def as_dict(self) -> dict[str, object]:
        return {
            "run_timestamp": self.run_timestamp,
            "run_date": self.run_date,
            "start_ts": self.start_ts.isoformat(),
            "end_ts": (self.end_ts or datetime.now()).isoformat(),
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "dry_run": self.dry_run,
            "return_code": self.return_code,
            "jobs": [result.as_dict() for result in self.results],
            "post_run_counts": self.post_run_counts,
        }

    def write_json(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.as_dict(), indent=2, default=str), encoding="utf-8")
        return target


def iter_tasks(nodes: Sequence[JobNode] = PIPELINE) -> Iterator[tuple[JobNode, tuple[str, ...]]]:
    """Yield ``(node, upstream_names)`` so a scheduler wrapper can build the same graph."""

    for node in nodes:
        yield node, node.depends_on


def describe(nodes: Sequence[JobNode] = PIPELINE) -> list[str]:
    """The ``--dry-run`` plan, mirroring the master script's dry-run listing."""

    lines: list[str] = []
    for phase, label in ((PHASE_BTEQ, "1. BTEQ"), (PHASE_SAS, "2. SAS ")):
        phase_nodes = [node.name for node in nodes if node.phase == phase]
        lines.append(f"  {label}: {' -> '.join(phase_nodes)}")
    lines.append("  3. Post: Validation & notification")
    for node in nodes:
        lines.append(
            f"    {node.name:<26} {node.legacy_source:<34} -> {node.target}"
            + (f"  (after {', '.join(node.depends_on)})" if node.depends_on else "")
        )
    return lines


def _skipped(node: JobNode, reason: str) -> JobResult:
    now = datetime.now()
    return JobResult(
        job_name=node.name,
        status=STATUS_SKIPPED,
        start_ts=now,
        end_ts=now,
        target_table=node.target,
        error=reason,
    )


def run_pipeline(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
    *,
    nodes: Sequence[JobNode] = PIPELINE,
    skip_phases: Iterable[str] = (),
    dry_run: bool = False,
    post_run_validation: bool = True,
    persist_audit: bool = True,
    resolve: Callable[[JobNode], JobRunner] = JobNode.runner,
) -> PipelineRun:
    """Execute the DAG with the legacy ordering and fail-fast semantics."""

    audit = audit if audit is not None else AuditLog(run_timestamp=config.run_timestamp)
    skipped_phases = set(skip_phases)
    run = PipelineRun(
        run_timestamp=config.run_timestamp, run_date=config.run_date_str, dry_run=dry_run
    )

    LOGGER.info(
        "Pipeline start | run_date=%s lookback=%s months",
        config.run_date_str,
        config.lookback_months,
    )
    if dry_run:
        for line in describe(nodes):
            LOGGER.info(line)
        run.end_ts = datetime.now()
        return run

    failed_phases: set[str] = set()
    # jobs that did not produce their output because something failed; a job skipped through
    # --skip-bteq/--skip-sas is *not* blocked, since those flags assert the output already exists
    blocked: set[str] = set()
    for node in nodes:
        if node.phase in skipped_phases:
            run.results.append(_skipped(node, f"phase {node.phase} skipped"))
            continue
        if node.phase in failed_phases:
            run.results.append(_skipped(node, f"aborted: {node.phase} phase failed upstream"))
            blocked.add(node.name)
            continue
        upstream_failure = next((name for name in node.depends_on if name in blocked), None)
        if upstream_failure is not None:
            run.results.append(_skipped(node, f"upstream {upstream_failure} did not succeed"))
            blocked.add(node.name)
            continue

        result = _run_node(node, spark, io, config, audit, resolve)
        run.results.append(result)
        if result.status == STATUS_FAILED:
            LOGGER.error("ABORT: %s failed (rc=%s)", node.name, result.return_code)
            blocked.add(node.name)
            failed_phases.add(node.phase)
            if node.phase == PHASE_BTEQ:
                # the master script exits before the SAS phase when BTEQ fails
                failed_phases.add(PHASE_SAS)

    if post_run_validation and not run.failed:
        run.post_run_counts = post_run_counts(io)
        LOGGER.info("Post-run row counts: %s", run.post_run_counts)

    if persist_audit:
        audit.flush(spark, io)

    run.end_ts = datetime.now()
    LOGGER.info(
        "Pipeline complete in %.1fs | rc=%s | failed=%s",
        run.elapsed_seconds,
        run.return_code,
        [result.job_name for result in run.failed],
    )
    return run


def _run_node(
    node: JobNode,
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog,
    resolve: Callable[[JobNode], JobRunner] = JobNode.runner,
) -> JobResult:
    start = datetime.now()
    LOGGER.info("START: %s (%s)", node.name, node.legacy_source)
    try:
        result = resolve(node)(spark, io, config, audit)
    except ValidationFailedError as exc:
        LOGGER.error("FAILED: %s validation aborted: %s", node.name, exc)
        return JobResult(
            job_name=node.name,
            status=STATUS_FAILED,
            start_ts=start,
            end_ts=datetime.now(),
            target_table=node.target,
            error=str(exc),
        )
    except Exception as exc:  # the driver converts any job failure into rc>=2
        LOGGER.exception("FAILED: %s", node.name)
        return JobResult(
            job_name=node.name,
            status=STATUS_FAILED,
            start_ts=start,
            end_ts=datetime.now(),
            target_table=node.target,
            error=f"{type(exc).__name__}: {exc}",
        )
    LOGGER.info(
        "%s: %s rows=%s in %.1fs",
        result.status,
        node.name,
        result.row_count,
        result.elapsed_seconds,
    )
    return result


def post_run_counts(io: DataIO) -> dict[str, int]:
    """Phase 3 of the master script: count the data product tables."""

    counts: dict[str, int] = {}
    for database, table in POST_RUN_TABLES:
        counts[f"{database}.{table}"] = (
            io.read_table(database, table).count() if io.table_exists(database, table) else -1
        )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser("retail banking analytics pipeline (PySpark port)")
    parser.add_argument("--skip-bteq", action="store_true", help="skip the staging phase")
    parser.add_argument("--skip-sas", action="store_true", help="skip the analytics phase")
    parser.add_argument("--dry-run", action="store_true", help="list the plan and exit")
    parser.add_argument("--only", action="append", default=None, help="run only these jobs")
    parser.add_argument("--metrics-json", default=None, help="write the run metrics to this path")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s"
    )
    config = config_from_args(args)
    nodes = (
        tuple(node for node in PIPELINE if node.name in set(args.only)) if args.only else PIPELINE
    )
    if args.only and not nodes:
        raise SystemExit(f"no jobs matched --only {args.only}")

    skip_phases = {
        phase for phase, flag in ((PHASE_BTEQ, args.skip_bteq), (PHASE_SAS, args.skip_sas)) if flag
    }

    if args.dry_run:
        run = PipelineRun(
            run_timestamp=config.run_timestamp, run_date=config.run_date_str, dry_run=True
        )
        print(f"DRY RUN - run_date={config.run_date_str} timestamp={config.run_timestamp}")
        for line in describe(nodes):
            print(line)
        run.end_ts = datetime.now()
        if args.metrics_json:
            run.write_json(args.metrics_json)
        return 0

    spark = build_spark_session("retail_banking_analytics", master=args.master)
    try:
        io = io_from_args(args, spark, config)
        audit = AuditLog(run_timestamp=config.run_timestamp)
        run = run_pipeline(spark, io, config, audit, nodes=nodes, skip_phases=skip_phases)
        if args.metrics_json:
            path = run.write_json(args.metrics_json)
            LOGGER.info("metrics written to %s", path)
        for result in run.results:
            LOGGER.info(
                "%-26s %-8s rows=%-8s %.1fs",
                result.job_name,
                result.status,
                result.row_count,
                result.elapsed_seconds,
            )
        return run.return_code
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
