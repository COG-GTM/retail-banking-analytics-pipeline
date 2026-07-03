"""Framework-agnostic pipeline DAG + executor.

Ports ``orchestration/run_full_pipeline.sh`` (+ ``bteq/run_bteq_pipeline.sh`` and
``sas/run_sas_pipeline.sh``):

* **BTEQ-before-SAS** phase gate: no SAS task starts until every BTEQ staging
  task has succeeded (the shell aborts the SAS phase if ``BTEQ_RC != 0``).
* **Intra-phase ordering** 01 -> 02 -> 03 (BTEQ) and 01 -> 02 -> 03 -> 04 (SAS),
  matching the sequential ``run_*_pipeline.sh`` scripts.
* **Fail-fast** equivalent to the shell ``rc >= 2`` checks: the first task that
  raises (including a :class:`~common.validation.ValidationError` from an
  ``%abort cancel``) aborts the run; no downstream task executes.

The task ``runner`` callables import their job module lazily so this module
imports cleanly even before every job exists, and so Airflow can import the DAG
without a SparkSession.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import DataIO

Runner = Callable[[SparkSession, DataIO, PipelineConfig, AuditLog], DataFrame]


class PipelineError(RuntimeError):
    """Raised when a task fails; aborts the run (fail-fast, shell rc>=2)."""


@dataclass(frozen=True)
class Task:
    name: str
    phase: str  # "BTEQ" | "SAS"
    target: str
    upstreams: tuple[str, ...]
    runner: Runner


# --- lazy job runners (import inside so the DAG imports without the jobs) ----
def _run_customer_360(spark, io, config, audit):
    from jobs import staging_customer_360 as m
    return m.run(spark, io, config, audit)


def _run_txn_summary(spark, io, config, audit):
    from jobs import staging_txn_summary as m
    return m.run(spark, io, config, audit)


def _run_risk_factors(spark, io, config, audit):
    from jobs import staging_risk_factors as m
    return m.run(spark, io, config, audit)


def _run_customer_segments(spark, io, config, audit):
    from jobs import dp_customer_segments as m
    return m.run(spark, io, config, audit)


def _run_txn_analytics(spark, io, config, audit):
    from jobs import dp_txn_analytics as m
    return m.run(spark, io, config, audit)


def _run_risk_scoring(spark, io, config, audit):
    from jobs import dp_risk_scoring as m
    return m.run(spark, io, config, audit)


def _run_master_profile(spark, io, config, audit):
    from jobs import dp_customer_master_profile as m
    return m.run(spark, io, config, audit)


def build_pipeline() -> list[Task]:
    """The seven-task DAG in declaration order (not necessarily run order)."""
    return [
        # ---- BTEQ staging phase (sequential 01 -> 02 -> 03) ----
        Task("01_stg_customer_360", "BTEQ", "STG_CUSTOMER_360", (), _run_customer_360),
        Task("02_stg_txn_summary", "BTEQ", "STG_TXN_SUMMARY", ("01_stg_customer_360",), _run_txn_summary),
        Task("03_stg_risk_factors", "BTEQ", "STG_RISK_FACTORS", ("02_stg_txn_summary",), _run_risk_factors),
        # ---- SAS analytics phase; gated on the last BTEQ task (phase gate),
        #      then sequential 01 -> 02 -> 03 -> 04 ----
        Task("01_customer_segments", "SAS", "CUSTOMER_SEGMENTS", ("03_stg_risk_factors",), _run_customer_segments),
        Task("02_txn_analytics", "SAS", "TRANSACTION_ANALYTICS", ("01_customer_segments",), _run_txn_analytics),
        Task("03_risk_scoring", "SAS", "CUSTOMER_RISK_SCORES", ("02_txn_analytics",), _run_risk_scoring),
        Task("04_customer_master_profile", "SAS", "CUSTOMER_MASTER_PROFILE", ("03_risk_scoring",), _run_master_profile),
    ]


def topological_order(tasks: list[Task]) -> list[Task]:
    """Kahn's algorithm; deterministic (preserves declaration order on ties).

    Raises :class:`PipelineError` on an unknown upstream or a dependency cycle.
    """
    by_name = {t.name: t for t in tasks}
    for t in tasks:
        for up in t.upstreams:
            if up not in by_name:
                raise PipelineError(f"task {t.name!r} has unknown upstream {up!r}")

    indegree = {t.name: len(t.upstreams) for t in tasks}
    ordered: list[Task] = []
    ready = [t for t in tasks if indegree[t.name] == 0]  # keeps declaration order
    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for t in tasks:
            if current.name in t.upstreams:
                indegree[t.name] -= 1
                if indegree[t.name] == 0:
                    ready.append(t)
    if len(ordered) != len(tasks):
        raise PipelineError("cycle detected in pipeline DAG")
    return ordered


@dataclass
class TaskResult:
    name: str
    status: str  # "SUCCESS" | "ERROR" | "SKIPPED"
    row_count: int | None = None
    error: str | None = None


@dataclass
class PipelineRun:
    run_id: str
    results: list[TaskResult] = field(default_factory=list)
    audit: AuditLog | None = None

    @property
    def succeeded(self) -> bool:
        return all(r.status == "SUCCESS" for r in self.results)


def dry_run_plan(tasks: list[Task] | None = None) -> list[str]:
    """List the execution steps without running (shell ``--dry-run``)."""
    tasks = tasks or build_pipeline()
    return [f"{t.phase}:{t.name} -> {t.target}" for t in topological_order(tasks)]


def run_pipeline(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    tasks: list[Task] | None = None,
    audit: AuditLog | None = None,
) -> PipelineRun:
    """Execute the DAG in dependency order with fail-fast semantics."""
    tasks = tasks or build_pipeline()
    order = topological_order(tasks)
    audit = audit or AuditLog(log_level=config.log_level)
    run = PipelineRun(run_id=audit.run_id, audit=audit)

    audit.log_step("PIPELINE", "START", f"run_date={config.run_date_str}")
    failed = False
    for task in order:
        if failed:
            run.results.append(TaskResult(task.name, "SKIPPED"))
            audit.log_step(task.name, "WARNING", "Skipped after upstream failure")
            continue
        try:
            df = task.runner(spark, io, config, audit)
            n = df.count() if df is not None else None
            run.results.append(TaskResult(task.name, "SUCCESS", row_count=n))
        except Exception as exc:  # fail-fast: abort remaining tasks
            failed = True
            run.results.append(TaskResult(task.name, "ERROR", error=str(exc)))
            audit.log_step(task.name, "ERROR", f"Task failed: {exc}")

    status = "SUCCESS" if run.succeeded else "ERROR"
    audit.log_step("PIPELINE", status, "Pipeline complete" if run.succeeded else "Pipeline aborted")
    if failed:
        first = next(r for r in run.results if r.status == "ERROR")
        raise PipelineError(f"pipeline aborted at {first.name}: {first.error}")
    return run
