"""The legacy config file drives the jobs - no constant may be re-typed in a job module."""

from __future__ import annotations

from datetime import date

import pytest

from common.config import ConfigError, PipelineConfig, RiskScoringConstants, parse_cfg_file

pytestmark = pytest.mark.nonfunctional

CFG_RELATIVE = "config/pipeline_config.cfg"


def test_parses_the_real_legacy_config(repo_root):
    values = parse_cfg_file(repo_root / CFG_RELATIVE, run_date=date(2026, 4, 10))

    assert values["LOOKBACK_MONTHS"] == "12"
    assert values["RISK_SCORE_THRESHOLD"] == "700"
    assert values["DB_CORE"] == "CORE_BANKING_DB"
    assert values["DB_TXN"] == "TXN_PROCESSING_DB"
    assert values["DB_STG"] == "ETL_STAGING_DB"
    assert values["DB_DP"] == "DATA_PRODUCTS_DB"
    assert values["RUN_DATE"] == "20260410"
    assert values["RUN_TIMESTAMP"].startswith("20260410_")
    # ${PIPELINE_HOME}/bteq must expand from an earlier key in the same file
    assert values["BTEQ_DIR"].endswith("/bteq")
    assert values["PIPELINE_HOME"] in values["BTEQ_DIR"]


def test_env_overrides_defaulted_variables(repo_root):
    values = parse_cfg_file(
        repo_root / CFG_RELATIVE,
        run_date=date(2026, 4, 10),
        environ={"TD_USERNAME": "svc_override"},
    )
    assert values["TD_USERNAME"] == "svc_override"

    defaulted = parse_cfg_file(repo_root / CFG_RELATIVE, run_date=date(2026, 4, 10), environ={})
    assert defaulted["TD_USERNAME"] == "svc_etl_pipeline"


def test_config_object_exposes_the_preserved_constants(repo_root):
    config = PipelineConfig.from_cfg_file(repo_root / CFG_RELATIVE, run_date=date(2026, 4, 10))

    assert config.lookback_months == 12
    assert config.risk_score_threshold == 700
    assert config.run_date == date(2026, 4, 10)
    assert config.run_date_str == "2026-04-10"
    assert config.log_level == "INFO"
    # %validate_table's production default
    assert config.min_rows == 1000


def test_risk_constants_match_the_sas_source():
    risk = RiskScoringConstants()

    weights = (
        risk.credit_risk_weight,
        risk.behaviour_risk_weight,
        risk.velocity_risk_weight,
        risk.bureau_score_weight,
        risk.payment_history_weight,
    )
    assert weights == (0.30, 0.25, 0.15, 0.20, 0.10)
    assert sum(weights) == pytest.approx(1.0)
    assert (
        risk.tier_low_max,
        risk.tier_moderate_max,
        risk.tier_elevated_max,
        risk.tier_high_max,
    ) == (
        20.0,
        40.0,
        60.0,
        80.0,
    )
    assert (risk.bureau_score_min, risk.bureau_score_max, risk.bureau_score_impute) == (
        300,
        850,
        680,
    )
    assert (risk.stepwise_slentry, risk.stepwise_slstay) == (0.10, 0.05)


def test_missing_file_and_missing_keys_are_reported(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        parse_cfg_file(tmp_path / "nope.cfg")

    partial = tmp_path / "partial.cfg"
    partial.write_text('export DB_CORE="X"\n', encoding="utf-8")
    with pytest.raises(ConfigError, match="missing required keys"):
        PipelineConfig.from_cfg_file(partial)


def test_unsupported_date_format_is_rejected(tmp_path):
    cfg = tmp_path / "bad.cfg"
    cfg.write_text("export RUN_DATE=$(date +%s)\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="unsupported date format"):
        parse_cfg_file(cfg)


def test_comments_and_non_export_lines_are_ignored(tmp_path):
    cfg = tmp_path / "c.cfg"
    cfg.write_text(
        "\n".join(
            [
                "# a comment",
                "NOT_AN_EXPORT=1",
                'export DB_CORE="CORE"  # trailing comment',
                "export DB_TXN=TXN # bare value",
                'export DB_STG="STG"',
                'export DB_DP="DP"',
                "export LOOKBACK_MONTHS=12",
                "export RISK_SCORE_THRESHOLD=700",
            ]
        ),
        encoding="utf-8",
    )
    config = PipelineConfig.from_cfg_file(cfg, run_date=date(2026, 1, 1), min_rows=5)

    assert "NOT_AN_EXPORT" not in config.raw
    assert (config.db_core, config.db_txn) == ("CORE", "TXN")
    assert config.min_rows == 5
    assert config.run_timestamp == "20260101_000000"
