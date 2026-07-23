"""Single source of truth for the pipeline DAG.

Both the local runner (:mod:`orchestration.local_runner`) and the Databricks
Asset Bundle job (``orchestration/databricks.yml``) describe the same dependency
graph.  Defining it once here lets the local runner execute it and lets the tests
assert the DAB definition stays in sync.

Legacy mapping: replaces the bash orchestrators
(``orchestration/run_full_pipeline.sh``, ``bteq/run_bteq_pipeline.sh``,
``sas/run_sas_pipeline.sh``).
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class Step:
    """One task in the pipeline DAG."""

    name: str
    module: str
    depends_on: tuple[str, ...] = ()

    def callable(self) -> Callable:
        return getattr(importlib.import_module(self.module), "run")


# Dependency order:
#   create_delta_tables
#     -> {stg_customer_360, stg_txn_summary, stg_risk_factors}
#     -> {customer_segments, txn_analytics, risk_scoring}
#     -> data_products
STEPS: tuple[Step, ...] = (
    Step("create_delta_tables", "ddl.create_delta_tables"),
    Step("stg_customer_360", "jobs.stg_customer_360", ("create_delta_tables",)),
    Step("stg_txn_summary", "jobs.stg_txn_summary", ("create_delta_tables",)),
    Step("stg_risk_factors", "jobs.stg_risk_factors", ("create_delta_tables",)),
    Step("customer_segments", "jobs.customer_segments", ("stg_customer_360",)),
    Step("txn_analytics", "jobs.txn_analytics", ("stg_txn_summary",)),
    Step("risk_scoring", "jobs.risk_scoring", ("stg_risk_factors",)),
    Step(
        "data_products",
        "jobs.data_products",
        ("customer_segments", "txn_analytics", "risk_scoring", "stg_customer_360"),
    ),
)

STEPS_BY_NAME: dict[str, Step] = {s.name: s for s in STEPS}


def topological_order() -> list[Step]:
    """Return steps in a dependency-respecting execution order."""
    ordered: list[Step] = []
    done: set[str] = set()
    remaining = list(STEPS)
    while remaining:
        progressed = False
        for step in list(remaining):
            if all(dep in done for dep in step.depends_on):
                ordered.append(step)
                done.add(step.name)
                remaining.remove(step)
                progressed = True
        if not progressed:
            raise ValueError(f"Cyclic or unsatisfiable dependencies: {remaining}")
    return ordered


def get_step(name: str) -> Optional[Step]:
    return STEPS_BY_NAME.get(name)
