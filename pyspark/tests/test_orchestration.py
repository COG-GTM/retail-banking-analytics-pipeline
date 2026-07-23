"""Validate the Databricks Asset Bundle definition and the pipeline DAG."""

from __future__ import annotations

from pathlib import Path

import yaml

from orchestration.pipeline import STEPS, STEPS_BY_NAME, topological_order

DAB_PATH = Path(__file__).resolve().parents[1] / "orchestration" / "databricks.yml"


def _load_dab():
    with open(DAB_PATH) as f:
        return yaml.safe_load(f)


def test_dab_parses_and_has_job():
    doc = _load_dab()
    assert doc["bundle"]["name"] == "retail-banking-analytics-pipeline"
    jobs = doc["resources"]["jobs"]
    assert "retail_banking_analytics_pipeline" in jobs


def test_dab_tasks_match_pipeline_steps():
    doc = _load_dab()
    tasks = doc["resources"]["jobs"]["retail_banking_analytics_pipeline"]["tasks"]
    task_keys = {t["task_key"] for t in tasks}
    assert task_keys == set(STEPS_BY_NAME)

    by_key = {t["task_key"]: t for t in tasks}
    for step in STEPS:
        task = by_key[step.name]
        deps = {d["task_key"] for d in task.get("depends_on", [])}
        assert deps == set(step.depends_on), f"depends_on mismatch for {step.name}"
        # Every task invokes run_job.py with its own --job name.
        params = task["spark_python_task"]["parameters"]
        assert params == ["--job", step.name]


def test_dab_depends_on_reference_existing_tasks():
    doc = _load_dab()
    tasks = doc["resources"]["jobs"]["retail_banking_analytics_pipeline"]["tasks"]
    keys = {t["task_key"] for t in tasks}
    for t in tasks:
        for dep in t.get("depends_on", []):
            assert dep["task_key"] in keys


def test_topological_order_is_valid():
    order = topological_order()
    seen: set[str] = set()
    for step in order:
        for dep in step.depends_on:
            assert dep in seen, f"{step.name} runs before dependency {dep}"
        seen.add(step.name)
    assert order[0].name == "create_delta_tables"
    assert order[-1].name == "data_products"


def test_every_step_module_exposes_run():
    for step in STEPS:
        assert callable(step.callable())
