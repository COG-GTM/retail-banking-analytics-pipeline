"""Run-scoped configuration for the PySpark analytics jobs.

Mirrors the env-var driven convention used by ``config/pipeline_config.cfg``:
every setting is sourced from an environment variable with a sensible default
derived from the repository layout, so there are no hard-coded warehouse names,
paths, connection strings, or secrets baked into the job code.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Repository root: <repo>/spark/config.py -> <repo>
REPO_ROOT = Path(__file__).resolve().parents[1]

_DEFAULT_STAGING = REPO_ROOT / "data" / "02_bteq_staging"
_DEFAULT_PRODUCTS = REPO_ROOT / "data" / "03_sas_data_products"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable, run-scoped configuration shared by all jobs."""

    # -- Data layer (replaces the connect_teradata LIBNAME references) --------
    # STGDB  -> staging_path   (ETL_STAGING_DB, read)
    # DPDB   -> products_path  (DATA_PRODUCTS_DB, read/write)
    staging_path: str
    products_path: str
    data_format: str  # csv | parquet | delta ...

    # -- Spark session -------------------------------------------------------
    spark_master: str
    app_name: str

    # -- Run metadata --------------------------------------------------------
    run_id: str
    effective_date: str      # yyyy-MM-dd  (SAS today())
    run_ts: str              # yyyy-MM-dd HH:mm:ss.SSSSSS (SAS datetime())
    reporting_period: str    # yyyy-MM     (SAS 02_txn_analytics reporting period)

    # -- Analytics hyper-parameters (mirror the SAS PROC options) ------------
    n_clusters: int          # PROC FASTCLUS maxclusters=5
    kmeans_max_iter: int     # PROC FASTCLUS maxiter=50
    kmeans_tol: float        # PROC FASTCLUS converge=0.001
    kmeans_seed: int         # deterministic seeding (FASTCLUS uses replace=full)
    rank_groups: int         # PROC RANK groups=100

    # -- Model version tags (SAS %let MODEL_VERSION) -------------------------
    seg_model_version: str
    txn_model_version: str
    risk_model_version: str
    master_model_version: str

    # -- Data-quality gate (SAS %validate_table min_rows=) -------------------
    min_rows: int

    # -- Business constants --------------------------------------------------
    default_bureau_score: int = 680     # SAS imputation placeholder
    interest_income_rate: float = 0.02  # SAS simplified interest proxy

    # convenience: extra spark conf as key=value pairs
    spark_conf: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        now = datetime.now()
        run_date = _env("RUN_DATE", now.strftime("%Y-%m-%d"))
        # reporting period = beginning-of-month of the run date (SAS intnx month)
        reporting_period = _env("REPORTING_PERIOD", run_date[:7])
        return cls(
            staging_path=_env("STAGING_PATH", str(_DEFAULT_STAGING)),
            products_path=_env("DATA_PRODUCTS_PATH", str(_DEFAULT_PRODUCTS)),
            data_format=_env("DATA_FORMAT", "csv"),
            spark_master=_env("SPARK_MASTER", "local[*]"),
            app_name=_env("SPARK_APP_NAME", "retail_banking_analytics"),
            run_id=_env("RUN_ID", now.strftime("%Y%m%d_%H%M%S")),
            effective_date=run_date,
            run_ts=_env("RUN_TS", now.strftime("%Y-%m-%d %H:%M:%S.%f")),
            reporting_period=reporting_period,
            n_clusters=int(_env("N_CLUSTERS", "5")),
            kmeans_max_iter=int(_env("KMEANS_MAX_ITER", "50")),
            kmeans_tol=float(_env("KMEANS_TOL", "0.001")),
            kmeans_seed=int(_env("KMEANS_SEED", "20240101")),
            rank_groups=int(_env("RANK_GROUPS", "100")),
            seg_model_version=_env("SEG_MODEL_VERSION", "SEG_V3.2"),
            txn_model_version=_env("TXN_MODEL_VERSION", "TXN_V2.1"),
            risk_model_version=_env("RISK_MODEL_VERSION", "RISK_V4.0"),
            master_model_version=_env("MASTER_MODEL_VERSION", "MASTER_V1.5"),
            # SAS used min_rows=1000; overridable so the smaller committed
            # sample (and unit tests) can run the same code path.
            min_rows=int(_env("PIPELINE_MIN_ROWS", "1000")),
        )

    # -- Dataset path helpers -----------------------------------------------
    def staging_dataset(self, name: str) -> str:
        return os.path.join(self.staging_path, name)

    def product_dataset(self, name: str) -> str:
        return os.path.join(self.products_path, name)
