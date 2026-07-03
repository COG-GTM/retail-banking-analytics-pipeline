"""Performance tier (opt-in, slow): scaled synthetic data + runtime SLAs.

Deselected by default (``-m performance``). Scale is configurable via env:
``PERF_CUSTOMERS`` (default 20_000) and ``PERF_SLA_SECONDS`` (default 900). The
legacy baselines are BTEQ ~20 min and SAS ~40 min end-to-end; the full pipeline
here must beat their combined wall-clock at the configured scale. For the
contractual 10M-customer target set ``PERF_CUSTOMERS=10000000`` on a cluster.
"""

from __future__ import annotations

import importlib
import os
import time

import pytest
from pyspark.sql import functions as F

from orchestration.pipeline import run_pipeline
from tests._synthetic import DictDataIO, synthetic_sources

pytestmark = pytest.mark.performance

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
def test_full_pipeline_scaled_runtime(spark, config):
    n = int(os.environ.get("PERF_CUSTOMERS", "20000"))
    sla = float(os.environ.get("PERF_SLA_SECONDS", "900"))
    txns = int(os.environ.get("PERF_TXNS_PER_ACCOUNT", "30"))

    sources = synthetic_sources(spark, config, n_customers=n, txns_per_account=txns)
    io = DictDataIO(spark, config, sources)

    start = time.time()
    run = run_pipeline(spark, io, config)
    elapsed = time.time() - start

    assert run.succeeded
    assert io.read_data_product("CUSTOMER_MASTER_PROFILE").count() > 0
    assert elapsed < sla, f"pipeline took {elapsed:.1f}s at {n:,} customers (SLA {sla}s)"


@requires_all_jobs
def test_no_excessive_skew_on_customer_join(spark, config):
    """Sanity check that partition sizes after the widest join are balanced
    (guards against the CUSTOMER_ID/ACCOUNT_ID skew called out in the brief)."""
    n = int(os.environ.get("PERF_CUSTOMERS", "20000"))
    sources = synthetic_sources(spark, config, n_customers=n, txns_per_account=10)
    io = DictDataIO(spark, config, sources)
    run_pipeline(spark, io, config)

    txn = io.read_data_product("TRANSACTION_ANALYTICS")
    counts = (
        txn.withColumn("_pid", F.spark_partition_id())
        .groupBy("_pid").count().collect()
    )
    sizes = sorted(r["count"] for r in counts)
    if len(sizes) > 2:
        # max partition should not be wildly larger than the median (skew guard)
        median = sizes[len(sizes) // 2]
        assert sizes[-1] <= max(median * 5, 1000)
