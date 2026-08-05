"""Locations of the committed sample extracts.

``data/01_source_tables`` holds the source system extract, ``data/02_bteq_staging`` and
``data/03_sas_data_products`` hold reference outputs of a previous run of the legacy pipeline.
The reference outputs are used by the regression tier only; they are never a logic source.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from common import schemas
from common.io import LocalDataIO
from common.schemas import TableSpec

REPO_ROOT = Path(__file__).resolve().parents[2]

SOURCE_FILES: dict[str, str] = {
    "CORE_BANKING_DB.CUSTOMERS": "data/01_source_tables/customers.csv",
    "CORE_BANKING_DB.ACCOUNTS": "data/01_source_tables/accounts.csv",
    "CORE_BANKING_DB.ADDRESSES": "data/01_source_tables/addresses.csv",
    "CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES": "data/01_source_tables/customer_bureau_scores.csv",
    "TXN_PROCESSING_DB.TRANSACTIONS": "data/01_source_tables/transactions.csv",
    "TXN_PROCESSING_DB.TRANSACTION_TYPES": "data/01_source_tables/transaction_types.csv",
}

REFERENCE_OUTPUT_FILES: dict[str, str] = {
    "ETL_STAGING_DB.STG_CUSTOMER_360": "data/02_bteq_staging/stg_customer_360.csv",
    "ETL_STAGING_DB.STG_TXN_SUMMARY": "data/02_bteq_staging/stg_txn_summary.csv",
    "ETL_STAGING_DB.STG_RISK_FACTORS": "data/02_bteq_staging/stg_risk_factors.csv",
    "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS": "data/03_sas_data_products/customer_segments.csv",
    "DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS": "data/03_sas_data_products/transaction_analytics.csv",
    "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES": "data/03_sas_data_products/customer_risk_scores.csv",
    "DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE": "data/03_sas_data_products/customer_master_profile.csv",
}

SOURCE_SPECS: tuple[TableSpec, ...] = (
    schemas.CUSTOMERS,
    schemas.ACCOUNTS,
    schemas.ADDRESSES,
    schemas.CUSTOMER_BUREAU_SCORES,
    schemas.TRANSACTIONS,
    schemas.TRANSACTION_TYPES,
)

#: The extract was taken on this date (``created_ts`` of every source row, and the last
#: transaction date), so it is the run date that reproduces the reference outputs.
SAMPLE_RUN_DATE = "2026-04-10"


def _overrides(repo_root: Path, mapping: dict[str, str]) -> dict[str, Path]:
    return {name: repo_root / relative for name, relative in mapping.items()}


def sample_source_io(spark: SparkSession, repo_root: Path | None = None) -> LocalDataIO:
    """A :class:`LocalDataIO` bound to the committed source extract."""

    root = Path(repo_root or REPO_ROOT)
    return LocalDataIO(
        spark=spark,
        base_path=root / "data",
        fmt="csv",
        path_overrides=_overrides(root, SOURCE_FILES),
    )


def reference_output_io(spark: SparkSession, repo_root: Path | None = None) -> LocalDataIO:
    """A :class:`LocalDataIO` bound to the committed reference outputs (regression tier only)."""

    root = Path(repo_root or REPO_ROOT)
    return LocalDataIO(
        spark=spark,
        base_path=root / "data",
        fmt="csv",
        path_overrides=_overrides(root, REFERENCE_OUTPUT_FILES),
    )
