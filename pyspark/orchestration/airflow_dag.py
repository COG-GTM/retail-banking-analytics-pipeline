"""Airflow representation of the pipeline DAG.

Mirrors :mod:`orchestration.pipeline`: BTEQ-before-SAS phase gate, intra-phase
01->02->03(->04) ordering, and fail-fast (Airflow stops downstream tasks when an
upstream task fails, equivalent to the shell ``rc>=2`` abort).

Airflow is an optional dependency -- importing this module without it is a no-op
so the rest of the package (and tests) never require Airflow to be installed.
The heavy lifting stays in the storage-agnostic job ``run()`` functions; each
Airflow task builds a Spark session + IO from Airflow ``Variable``/env config.
"""

from __future__ import annotations

import datetime as _dt

from orchestration.pipeline import Task, build_pipeline, topological_order

try:  # pragma: no cover - exercised only where Airflow is installed
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    _AIRFLOW_AVAILABLE = True
except Exception:  # pragma: no cover
    _AIRFLOW_AVAILABLE = False


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": _dt.timedelta(minutes=5),
    # fail-fast: don't let a downstream task run if an upstream failed
    "trigger_rule": "all_success",
}


def _task_callable(task: Task):  # pragma: no cover - runs inside Airflow workers
    def _execute(**context):
        from common.config import PipelineConfig
        from common.io import LocalDataIO
        from common.spark import build_spark

        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        config = PipelineConfig.from_env().with_overrides(
            **({"run_date": _dt.date.fromisoformat(conf["run_date"])} if conf.get("run_date") else {})
        )
        spark = build_spark(task.name)
        io = LocalDataIO(spark, config, conf["source_dir"], conf["lake_dir"])
        task.runner(spark, io, config, None)

    return _execute


def build_dag(dag_id: str = "retail_banking_analytics_pipeline"):  # pragma: no cover
    """Construct the Airflow DAG. Returns ``None`` if Airflow is unavailable."""
    if not _AIRFLOW_AVAILABLE:
        return None

    tasks = build_pipeline()
    topological_order(tasks)  # validate the graph at import time

    with DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule="0 2 * * *",  # daily 02:00, after which BTEQ then SAS run
        start_date=_dt.datetime(2026, 1, 1),
        catchup=False,
        tags=["retail-banking", "teradata-migration", "pyspark"],
    ) as dag:
        operators = {
            t.name: PythonOperator(task_id=t.name, python_callable=_task_callable(t))
            for t in tasks
        }
        for t in tasks:
            for up in t.upstreams:
                operators[up] >> operators[t.name]
    return dag


# Airflow discovers a module-level DAG object named ``dag``.
dag = build_dag() if _AIRFLOW_AVAILABLE else None
