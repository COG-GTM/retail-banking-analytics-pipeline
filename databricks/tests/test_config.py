"""Unit tests for the job-parameter configuration layer."""

from __future__ import annotations

from datetime import date

import pytest

from shared.config import PARAM_DEFAULTS, PipelineConfig, exit_if_skipped


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in PARAM_DEFAULTS:
        monkeypatch.delenv(f"RBA_{name.upper()}", raising=False)


def test_defaults_match_legacy_pipeline_config() -> None:
    cfg = PipelineConfig.from_widgets()
    assert cfg.lookback_months == 12
    assert cfg.risk_score_threshold == 700
    assert cfg.run_date == date.today()
    assert cfg.table_format == "delta"
    assert cfg.run_id


def test_env_overrides_are_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RBA_CATALOG", "prod_banking")
    monkeypatch.setenv("RBA_GOLD_SCHEMA", "products")
    monkeypatch.setenv("RBA_RUN_DATE", "2026-04-10")
    monkeypatch.setenv("RBA_LOOKBACK_MONTHS", "6")

    cfg = PipelineConfig.from_widgets()

    assert cfg.gold("CUSTOMER_SEGMENTS") == "prod_banking.products.CUSTOMER_SEGMENTS"
    assert cfg.run_date == date(2026, 4, 10)
    assert cfg.run_date_literal == "DATE '2026-04-10'"
    assert cfg.reporting_period == "2026-04"
    assert cfg.lookback_months == 6


@pytest.mark.parametrize(
    ("param", "layer"),
    [("RBA_SKIP_BTEQ", "silver"), ("RBA_SKIP_SAS", "gold")],
)
def test_legacy_skip_aliases(monkeypatch: pytest.MonkeyPatch, param: str, layer: str) -> None:
    """``--skip-bteq`` / ``--skip-sas`` from run_full_pipeline.sh still work."""
    monkeypatch.setenv(param, "true")
    cfg = PipelineConfig.from_widgets()
    assert exit_if_skipped(cfg, layer) is True
    assert exit_if_skipped(cfg, "bronze") is False


def test_landing_path_defaults_to_a_unity_catalog_volume() -> None:
    cfg = PipelineConfig(catalog="rb", bronze_schema="bronze")
    assert cfg.landing_path == "/Volumes/rb/bronze/landing/01_source_tables"
    assert PipelineConfig(source_data_path="/tmp/src/").landing_path == "/tmp/src"


def test_table_helpers_use_three_level_namespace() -> None:
    cfg = PipelineConfig(catalog="rb", silver_schema="s", ops_schema="o")
    assert cfg.silver("STG_CUSTOMER_360") == "rb.s.STG_CUSTOMER_360"
    assert cfg.ops("ETL_RUN_LOG") == "rb.o.ETL_RUN_LOG"


def test_no_credentials_in_defaults() -> None:
    """The SAS {SAS004} passwords must not have survived the migration."""
    joined = " ".join(PARAM_DEFAULTS.values()).lower()
    for forbidden in ("sas004", "password", "passwd", "ldap"):
        assert forbidden not in joined
