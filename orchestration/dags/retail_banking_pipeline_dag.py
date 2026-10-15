"""
Retail Banking Analytics Pipeline - Airflow DAG (local DuckDB / pandas / scikit-learn).

Replaces orchestration/run_full_pipeline.sh (Teradata BTEQ + SAS) with one task per
legacy step, all writing to a persisted DuckDB file:

    generate_source_data -> stg_customer_360 -> stg_txn_summary -> stg_risk_factors
      -> customer_segments -> txn_analytics -> risk_scoring -> data_products
      -> validate_row_counts

Failure semantics mirror the shell script:
  - any exception fails the task (BTEQ `.IF ERRORCODE <> 0` / SAS rc >= 2)
  - downstream tasks only run on upstream success (`set -e` abort-on-failure)
  - validate_row_counts fails if a data product table is empty (`.IF ACTIVITYCOUNT = 0`)

Configuration (environment variables):
  RETAIL_BANKING_DUCKDB_PATH   DuckDB file (default: local/duckdb/airflow_local.db)
  RETAIL_BANKING_N_CUSTOMERS   synthetic customers to generate (default: 5000)
  RETAIL_BANKING_SEED          random seed (default: 42)
  RETAIL_BANKING_RUN_DATE      set per task from the run's logical date (`ds`); stamps effective_date
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

ENGINE_DIR = Path(__file__).resolve().parents[2] / "local" / "duckdb"
if str(ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(ENGINE_DIR))


def _run_step(step: str, **kwargs) -> dict | int:
    """Open the shared DuckDB file and run one engine step (imported lazily to keep DAG parsing fast)."""
    import run_demo
    from airflow.operators.python import get_current_context

    # One logical date per DAG run, so effective_date matches across tasks even across midnight.
    os.environ[run_demo.RUN_DATE_ENV] = get_current_context()["ds"]
    with run_demo.connect() as con:
        return getattr(run_demo, step)(con, **kwargs)


@dag(
    dag_id="retail_banking_pipeline",
    description="Retail banking analytics: synthetic sources -> DuckDB staging -> pandas/sklearn data products",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["retail-banking", "duckdb", "local"],
    doc_md=__doc__,
)
def retail_banking_pipeline():
    @task
    def generate_source_data() -> dict:
        return _run_step(
            "generate_source_data",
            n_customers=int(os.environ.get("RETAIL_BANKING_N_CUSTOMERS", "5000")),
            seed=int(os.environ.get("RETAIL_BANKING_SEED", "42")),
        )

    # Phase 1: BTEQ staging ports (DuckDB SQL)
    @task
    def stg_customer_360() -> int:
        return _run_step("stg_customer_360")

    @task
    def stg_txn_summary() -> int:
        return _run_step("stg_txn_summary")

    @task
    def stg_risk_factors() -> int:
        return _run_step("stg_risk_factors")

    # Phase 2: SAS analytics ports (pandas / scikit-learn)
    @task
    def customer_segments() -> int:
        return _run_step("customer_segments")

    @task
    def txn_analytics() -> int:
        return _run_step("transaction_analytics")

    @task
    def risk_scoring() -> int:
        return _run_step("customer_risk_scores")

    @task
    def data_products() -> int:
        return _run_step("customer_master_profile")

    # Phase 3: row-count validation
    @task
    def validate_row_counts() -> dict:
        counts = _run_step("phase4_validate")
        for table, rows in counts.items():
            print(f"VALIDATION {table}: {rows} rows")
        empty = [t for t, c in counts.items() if c == 0]
        if empty:
            raise AirflowFailException(f"Data product tables are empty: {empty}")
        return counts

    (
        generate_source_data()
        >> stg_customer_360()
        >> stg_txn_summary()
        >> stg_risk_factors()
        >> customer_segments()
        >> txn_analytics()
        >> risk_scoring()
        >> data_products()
        >> validate_row_counts()
    )


retail_banking_pipeline()
