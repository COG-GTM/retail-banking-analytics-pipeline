"""Ticket 2 - configuration & secrets."""
from __future__ import annotations

from datetime import date

import pytest

from common.config import (
    DEFAULT_CATALOG,
    LEGACY_DATABASE_MAP,
    PipelineConfig,
    load_config,
)


def test_legacy_database_map_covers_all_four_teradata_dbs():
    assert LEGACY_DATABASE_MAP == {
        "CORE_BANKING_DB": "core_banking",
        "TXN_PROCESSING_DB": "txn_processing",
        "ETL_STAGING_DB": "etl_staging",
        "DATA_PRODUCTS_DB": "data_products",
    }


def test_table_helpers_produce_three_level_unity_catalog_names():
    cfg = PipelineConfig(catalog="cat")
    assert cfg.core("customers") == "cat.core_banking.customers"
    assert cfg.txn("transactions") == "cat.txn_processing.transactions"
    assert cfg.staging("stg_customer_360") == "cat.etl_staging.stg_customer_360"
    assert cfg.product("customer_segments") == "cat.data_products.customer_segments"
    assert cfg.schemas == [
        "core_banking", "txn_processing", "etl_staging", "data_products",
    ]


def test_load_config_defaults_and_overrides():
    cfg = load_config()
    assert cfg.catalog == DEFAULT_CATALOG
    assert cfg.lookback_months == 12
    assert cfg.risk_score_threshold == 700

    overridden = load_config(catalog="spark_catalog", run_date=date(2026, 4, 10))
    assert overridden.catalog == "spark_catalog"
    assert overridden.run_date == date(2026, 4, 10)


def test_widget_reads_from_environment(monkeypatch):
    monkeypatch.setenv("LOOKBACK_MONTHS", "6")
    monkeypatch.setenv("CATALOG", "env_catalog")
    cfg = load_config()
    assert cfg.lookback_months == 6
    assert cfg.catalog == "env_catalog"


def test_run_date_widget_is_parsed(monkeypatch):
    monkeypatch.setenv("RUN_DATE", "2026-01-15")
    cfg = load_config()
    assert cfg.run_date == date(2026, 1, 15)


def test_secret_resolves_from_env_then_raises(monkeypatch):
    cfg = PipelineConfig(secret_scope="scope")
    monkeypatch.setenv("RBA_SECRET_TD_PASSWORD", "s3cr3t")
    assert cfg.secret("td_password") == "s3cr3t"

    with pytest.raises(KeyError):
        cfg.secret("does_not_exist")


def test_no_hardcoded_sas004_credentials_in_source():
    """The legacy {SAS004} password *literals* must not survive under databricks/."""
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    # Match an actual credential assignment, e.g. password="{SAS004}XXXX" - not a
    # docstring merely *mentioning* that such literals were removed.
    cred = re.compile(r"""password\s*=\s*["']\{SAS004\}""")
    this_file = pathlib.Path(__file__).resolve()
    offenders = [
        str(p)
        for p in root.rglob("*.py")
        if p.resolve() != this_file and cred.search(p.read_text())
    ]
    assert offenders == []
