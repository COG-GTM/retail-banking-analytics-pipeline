"""End-to-end: run the full DAG on a synthetic medium dataset and validate all
four data products.

Skips until every child job module is integrated (the orchestrator imports jobs
lazily, so this file always collects cleanly).
"""

from __future__ import annotations

import importlib

import pytest

from common import schemas
from orchestration.pipeline import build_pipeline, run_pipeline
from tests._synthetic import DictDataIO, synthetic_sources

pytestmark = pytest.mark.e2e

_JOB_MODULES = [
    "jobs.staging_customer_360", "jobs.staging_txn_summary", "jobs.staging_risk_factors",
    "jobs.dp_customer_segments", "jobs.dp_txn_analytics", "jobs.dp_risk_scoring",
    "jobs.dp_customer_master_profile",
]


def _all_jobs_available() -> bool:
    for m in _JOB_MODULES:
        try:
            importlib.import_module(m)
        except Exception:
            return False
    return True


requires_all_jobs = pytest.mark.skipif(
    not _all_jobs_available(), reason="not all job modules integrated yet"
)


@requires_all_jobs
def test_full_dag_produces_all_products(spark, config):
    # >1000 active customers so jobs with the SAS min_rows=1000 gate pass.
    sources = synthetic_sources(spark, config, n_customers=1500, txns_per_account=20)
    io = DictDataIO(spark, config, sources)

    run = run_pipeline(spark, io, config)
    assert run.succeeded
    assert len(run.results) == len(build_pipeline())

    products = {
        "CUSTOMER_SEGMENTS": schemas.CUSTOMER_SEGMENTS,
        "TRANSACTION_ANALYTICS": schemas.TRANSACTION_ANALYTICS,
        "CUSTOMER_RISK_SCORES": schemas.CUSTOMER_RISK_SCORES,
        "CUSTOMER_MASTER_PROFILE": schemas.CUSTOMER_MASTER_PROFILE,
    }
    for name, spec in products.items():
        df = io.read_data_product(name)
        schemas.assert_schema(df, spec)
        assert df.count() > 0
        # primary key uniqueness on the golden record + segments/risk
        assert df.groupBy("customer_id").count().filter("count > 1").count() == 0

    # The master profile is the union base: one row per active customer.
    master = io.read_data_product("CUSTOMER_MASTER_PROFILE")
    active = sources["CUSTOMERS"].filter("customer_status = 'A'").count()
    assert master.count() == active
