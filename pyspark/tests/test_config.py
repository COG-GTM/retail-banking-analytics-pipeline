"""Unit tests for the shared Config contract."""

from __future__ import annotations

import datetime as dt

from common.config import Config, get_config


def test_defaults_match_contract():
    cfg = Config()
    assert cfg.catalog == "retail_banking"
    assert cfg.schema_core == "core_banking"
    assert cfg.schema_txn == "txn_processing"
    assert cfg.schema_stg == "etl_staging"
    assert cfg.schema_dp == "data_products"
    assert cfg.lookback_months == 12
    assert cfg.risk_score_threshold == 700
    assert cfg.log_level == "INFO"
    assert cfg.secret_scope == "retail_banking"


def test_table_helper_builds_three_level_name():
    cfg = Config()
    assert cfg.table(cfg.schema_stg, "stg_customer_360") == (
        "retail_banking.etl_staging.stg_customer_360"
    )
    assert cfg.table(cfg.schema_dp, "customer_segments") == (
        "retail_banking.data_products.customer_segments"
    )


def test_schemas_property_covers_four_databases():
    cfg = Config()
    assert cfg.schemas == (
        "core_banking",
        "txn_processing",
        "etl_staging",
        "data_products",
    )


def test_config_is_frozen():
    cfg = Config()
    try:
        cfg.catalog = "other"  # type: ignore[misc]
    except Exception as exc:  # noqa: BLE001
        assert exc.__class__.__name__ == "FrozenInstanceError"
    else:  # pragma: no cover
        raise AssertionError("Config should be immutable")


def test_get_config_reads_env_fallback(monkeypatch):
    monkeypatch.setenv("CATALOG", "unit_cat")
    monkeypatch.setenv("SCHEMA_STG", "stg_override")
    monkeypatch.setenv("LOOKBACK_MONTHS", "6")
    monkeypatch.setenv("RISK_SCORE_THRESHOLD", "650")
    monkeypatch.setenv("RUN_DATE", "2026-04-10")

    cfg = get_config()

    assert cfg.catalog == "unit_cat"
    assert cfg.schema_stg == "stg_override"
    assert cfg.lookback_months == 6
    assert cfg.risk_score_threshold == 650
    assert cfg.run_date == dt.date(2026, 4, 10)


def test_get_secret_env_fallback(monkeypatch):
    monkeypatch.setenv("TD_PASSWORD", "s3cr3t")
    cfg = Config()
    assert cfg.get_secret("TD_PASSWORD") == "s3cr3t"
    assert cfg.get_secret("MISSING_KEY", default="fallback") == "fallback"
