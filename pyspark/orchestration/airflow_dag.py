"""Optional Airflow/Databricks wiring generated from the same graph as the driver.

The scheduler wrapper is deliberately thin: it only translates :data:`pipeline.PIPELINE` into
tasks and dependencies, so the legacy ordering has exactly one definition. Importing this module
without Airflow installed is a no-op (``build_dag`` raises), which keeps the test suite free of an
Airflow dependency.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from orchestration.pipeline import PIPELINE, JobNode, iter_tasks

if TYPE_CHECKING:  # pragma: no cover - typing only
    from airflow import DAG

DEFAULT_DAG_ID = "retail_banking_analytics"


def spark_submit_command(node: JobNode, *, extra_args: Sequence[str] = ()) -> list[str]:
    """The ``spark-submit`` invocation for one job, usable from any scheduler."""

    return [
        "spark-submit",
        "--name",
        node.name,
        "-m",
        node.module,
        *extra_args,
    ]


def task_specifications(
    nodes: Sequence[JobNode] = PIPELINE, *, extra_args: Sequence[str] = ()
) -> list[dict[str, object]]:
    """Scheduler-agnostic task list: id, command, upstream ids."""

    return [
        {
            "task_id": node.name,
            "phase": node.phase,
            "command": spark_submit_command(node, extra_args=extra_args),
            "upstream": list(upstream),
            "target": node.target,
        }
        for node, upstream in iter_tasks(nodes)
    ]


def build_dag(
    dag_id: str = DEFAULT_DAG_ID,
    *,
    nodes: Sequence[JobNode] = PIPELINE,
    extra_args: Sequence[str] = (),
    **dag_kwargs: object,
) -> DAG:  # pragma: no cover - requires Airflow
    """Build an Airflow DAG mirroring the legacy dependency graph."""

    from airflow import DAG
    from airflow.operators.bash import BashOperator

    dag = DAG(dag_id, **dag_kwargs)
    tasks = {}
    for spec in task_specifications(nodes, extra_args=extra_args):
        tasks[spec["task_id"]] = BashOperator(
            task_id=str(spec["task_id"]),
            bash_command=" ".join(str(part) for part in spec["command"]),
            dag=dag,
        )
    for spec in task_specifications(nodes, extra_args=extra_args):
        for upstream in spec["upstream"]:
            tasks[str(upstream)] >> tasks[str(spec["task_id"])]
    return dag
