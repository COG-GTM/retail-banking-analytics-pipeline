"""Unit tests for pyspark.common.config."""

from __future__ import annotations

from datetime import date

import pytest

from common.config import (
    Config,
    MissingSecretError,
    get_config,
    get_secret,
)


class _FakeWidgets:
    def __init__(self, values):
        self._values = values

    def get(self, key):
        if key in self._values:
            return self._values[key]
        raise Exception(f"widget '{key}' not found")


class _FakeSecrets:
    def __init__(self, store):
        self._store = store

    def get(self, scope, key):
        return self._store[(scope, key)]


class _FakeDbutils:
    def __init__(self, widgets=None, secrets=None):
        self.widgets = _FakeWidgets(widgets or {})
        self.secrets = _FakeSecrets(secrets or {})


# -- get_config defaults ----------------------------------------------------


def test_get_config_defaults():
    cfg = get_config(dbutils=None, env={})
    assert cfg.catalog == "retail_banking"
    assert cfg.schema_core == "core_banking"
    assert cfg.schema_txn == "txn_processing"
    assert cfg.schema_stg == "etl_staging"
    assert cfg.schema_dp == "data_products"
    assert cfg.lookback_months == 12
    assert cfg.risk_score_threshold == 700
    assert cfg.log_level == "INFO"
    assert cfg.secret_scope == "retail_banking"


def test_run_date_defaults_to_today():
    cfg = get_config(dbutils=None, env={})
    assert cfg.run_date == date.today().isoformat()


def test_config_is_frozen():
    cfg = get_config(dbutils=None, env={})
    with pytest.raises(Exception):
        cfg.catalog = "other"  # type: ignore[misc]


# -- env-var overrides ------------------------------------------------------


def test_env_var_overrides():
    env = {
        "PIPELINE_CATALOG": "prod_catalog",
        "PIPELINE_SCHEMA_STG": "stg_override",
        "PIPELINE_LOOKBACK_MONTHS": "6",
        "PIPELINE_RISK_SCORE_THRESHOLD": "750",
        "PIPELINE_RUN_DATE": "2026-01-15",
        "PIPELINE_LOG_LEVEL": "DEBUG",
        "PIPELINE_SECRET_SCOPE": "custom_scope",
    }
    cfg = get_config(dbutils=None, env=env)
    assert cfg.catalog == "prod_catalog"
    assert cfg.schema_stg == "stg_override"
    assert cfg.lookback_months == 6
    assert cfg.risk_score_threshold == 750
    assert cfg.run_date == "2026-01-15"
    assert cfg.log_level == "DEBUG"
    assert cfg.secret_scope == "custom_scope"


def test_int_override_invalid_raises():
    with pytest.raises(ValueError):
        get_config(dbutils=None, env={"PIPELINE_LOOKBACK_MONTHS": "not-an-int"})


def test_empty_env_value_falls_back_to_default():
    cfg = get_config(dbutils=None, env={"PIPELINE_CATALOG": ""})
    assert cfg.catalog == "retail_banking"


# -- Databricks widget precedence -------------------------------------------


def test_widget_takes_precedence_over_env():
    dbutils = _FakeDbutils(widgets={"catalog": "widget_catalog"})
    env = {"PIPELINE_CATALOG": "env_catalog"}
    cfg = get_config(dbutils=dbutils, env=env)
    assert cfg.catalog == "widget_catalog"


def test_env_used_when_widget_missing():
    dbutils = _FakeDbutils(widgets={})
    env = {"PIPELINE_CATALOG": "env_catalog"}
    cfg = get_config(dbutils=dbutils, env=env)
    assert cfg.catalog == "env_catalog"


# -- Config.table -----------------------------------------------------------


def test_config_table_builds_fqn():
    cfg = get_config(dbutils=None, env={})
    assert (
        cfg.table(cfg.schema_stg, "stg_customer_360")
        == "retail_banking.etl_staging.stg_customer_360"
    )
    assert (
        cfg.table(cfg.schema_dp, "customer_master_profile")
        == "retail_banking.data_products.customer_master_profile"
    )


def test_config_table_respects_catalog_override():
    cfg = get_config(dbutils=None, env={"PIPELINE_CATALOG": "prod"})
    assert cfg.table("data_products", "customer_segments") == (
        "prod.data_products.customer_segments"
    )


@pytest.mark.parametrize("schema,name", [("", "t"), ("s", ""), ("", "")])
def test_config_table_requires_both_parts(schema, name):
    cfg = Config()
    with pytest.raises(ValueError):
        cfg.table(schema, name)


# -- get_secret -------------------------------------------------------------


def test_get_secret_env_fallback():
    cfg = get_config(dbutils=None, env={})
    value = get_secret(cfg, "td_password", dbutils=None, env={"td_password": "s3cret"})
    assert value == "s3cret"


def test_get_secret_prefers_dbutils_scope():
    cfg = get_config(dbutils=None, env={})
    dbutils = _FakeDbutils(secrets={("retail_banking", "td_password"): "from-scope"})
    value = get_secret(
        cfg, "td_password", dbutils=dbutils, env={"td_password": "from-env"}
    )
    assert value == "from-scope"


def test_get_secret_falls_back_to_env_when_scope_missing():
    cfg = get_config(dbutils=None, env={})
    dbutils = _FakeDbutils(secrets={})
    value = get_secret(
        cfg, "td_password", dbutils=dbutils, env={"td_password": "from-env"}
    )
    assert value == "from-env"


def test_get_secret_missing_raises():
    cfg = get_config(dbutils=None, env={})
    with pytest.raises(MissingSecretError):
        get_secret(cfg, "does_not_exist", dbutils=None, env={})


def test_get_secret_uses_default_when_missing():
    cfg = get_config(dbutils=None, env={})
    value = get_secret(cfg, "optional_key", dbutils=None, env={}, default="fallback")
    assert value == "fallback"


def test_get_secret_uses_configured_scope():
    cfg = get_config(dbutils=None, env={"PIPELINE_SECRET_SCOPE": "custom"})
    dbutils = _FakeDbutils(secrets={("custom", "api_key"): "abc"})
    assert get_secret(cfg, "api_key", dbutils=dbutils, env={}) == "abc"
