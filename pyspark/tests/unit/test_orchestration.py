"""Unit tests for the DAG structure + executor (no Spark jobs needed)."""

from __future__ import annotations

import pytest

from orchestration import pipeline as P

pytestmark = pytest.mark.unit


def _dummy(name, upstreams, runner):
    return P.Task(name, "BTEQ", name.upper(), tuple(upstreams), runner)


def test_pipeline_has_seven_tasks_and_valid_order():
    tasks = P.build_pipeline()
    assert len(tasks) == 7
    order = [t.name for t in P.topological_order(tasks)]
    # BTEQ before SAS, and intra-phase ordering preserved
    assert order == [
        "01_stg_customer_360", "02_stg_txn_summary", "03_stg_risk_factors",
        "01_customer_segments", "02_txn_analytics", "03_risk_scoring",
        "04_customer_master_profile",
    ]


def test_bteq_before_sas_gate():
    tasks = P.build_pipeline()
    order = P.topological_order(tasks)
    last_bteq = max(i for i, t in enumerate(order) if t.phase == "BTEQ")
    first_sas = min(i for i, t in enumerate(order) if t.phase == "SAS")
    assert last_bteq < first_sas


def test_topological_order_detects_cycle():
    a = _dummy("a", ["b"], lambda *_: None)
    b = _dummy("b", ["a"], lambda *_: None)
    with pytest.raises(P.PipelineError):
        P.topological_order([a, b])


def test_topological_order_unknown_upstream():
    a = _dummy("a", ["missing"], lambda *_: None)
    with pytest.raises(P.PipelineError):
        P.topological_order([a])


def test_dry_run_plan_lists_all_steps():
    plan = P.dry_run_plan()
    assert len(plan) == 7
    assert plan[0].startswith("BTEQ:01_stg_customer_360")
    assert plan[-1].startswith("SAS:04_customer_master_profile")


class _FakeDF:
    def __init__(self, n):
        self._n = n

    def count(self):
        return self._n


def test_run_pipeline_success(spark, config):
    calls = []

    def make(n):
        def _r(s, io, c, audit):
            calls.append(n)
            return _FakeDF(n)
        return _r

    tasks = [
        _dummy("t1", [], make(10)),
        _dummy("t2", ["t1"], make(20)),
    ]
    run = P.run_pipeline(spark, io=None, config=config, tasks=tasks)
    assert run.succeeded
    assert [r.row_count for r in run.results] == [10, 20]
    assert calls == [10, 20]


def test_run_pipeline_fail_fast_skips_downstream(spark, config):
    executed = []

    def ok(s, io, c, audit):
        executed.append("ok")
        return _FakeDF(1)

    def boom(s, io, c, audit):
        executed.append("boom")
        raise ValueError("kaboom")

    def should_not_run(s, io, c, audit):
        executed.append("downstream")
        return _FakeDF(1)

    tasks = [
        _dummy("t1", [], ok),
        _dummy("t2", ["t1"], boom),
        _dummy("t3", ["t2"], should_not_run),
    ]
    with pytest.raises(P.PipelineError):
        P.run_pipeline(spark, io=None, config=config, tasks=tasks)
    assert "downstream" not in executed  # fail-fast; the raised error asserts abort
