"""Cover the Databricks-only and entry-point branches of the shared modules.

These paths (``dbutils`` widgets/secrets, the ``get_spark`` active-session reuse,
and the ``main`` entry point) do not run on local Spark, so they are exercised
here with a stubbed ``dbutils`` to keep the owned modules well covered.
"""

from __future__ import annotations

import common.config as config_mod
from common import spark as spark_mod
from ddl import create_delta_tables


class _FakeWidgets:
    def __init__(self, values):
        self._values = values

    def get(self, key):
        if key in self._values:
            return self._values[key]
        raise KeyError(key)


class _FakeSecrets:
    def __init__(self, values):
        self._values = values

    def get(self, scope, key):  # noqa: A003 - mirrors dbutils API
        return self._values[f"{scope}/{key}"]


class _FakeDbutils:
    def __init__(self, widgets=None, secrets=None):
        self.widgets = _FakeWidgets(widgets or {})
        self.secrets = _FakeSecrets(secrets or {})


def test_get_config_reads_dbutils_widgets(monkeypatch):
    fake = _FakeDbutils(widgets={"catalog": "uc_prod", "lookback_months": "3"})
    monkeypatch.setattr(config_mod, "_get_dbutils", lambda: fake)

    cfg = config_mod.get_config()

    assert cfg.catalog == "uc_prod"
    assert cfg.lookback_months == 3
    # A widget that raises KeyError falls back to the default.
    assert cfg.schema_core == "core_banking"


def test_get_secret_reads_dbutils_scope(monkeypatch):
    fake = _FakeDbutils(secrets={"retail_banking/TD_PASSWORD": "from-scope"})
    monkeypatch.setattr(config_mod, "_get_dbutils", lambda: fake)

    cfg = config_mod.Config()
    assert cfg.get_secret("TD_PASSWORD") == "from-scope"


def test_get_secret_falls_back_when_scope_missing(monkeypatch):
    fake = _FakeDbutils(secrets={})  # .get raises KeyError -> env fallback
    monkeypatch.setattr(config_mod, "_get_dbutils", lambda: fake)
    monkeypatch.setenv("OTHER_SECRET", "env-val")

    cfg = config_mod.Config()
    assert cfg.get_secret("OTHER_SECRET") == "env-val"


def test_get_dbutils_returns_none_locally():
    # No IPython dbutils and no Databricks runtime -> None.
    assert config_mod._get_dbutils() is None


def test_get_spark_reuses_active_session(spark):
    assert spark_mod.get_spark() is spark


def test_main_runs_bootstrap(monkeypatch, spark, cfg):
    monkeypatch.setattr("common.config.get_config", lambda: cfg)
    monkeypatch.setattr("common.spark.get_spark", lambda app_name="x": spark)

    create_delta_tables.main()

    fqn = cfg.table(cfg.schema_dp, "customer_risk_scores")
    assert spark.table(fqn).count() == 0
