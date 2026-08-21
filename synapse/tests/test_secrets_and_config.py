import pytest

from pipeline_utils.config import PipelineConfig, SnowflakeConfig
from pipeline_utils.secrets import SecretResolver


class FakeSecret:
    def __init__(self, value):
        self.value = value


class FakeSecretClient:
    def __init__(self, secrets):
        self._secrets = secrets
        self.calls = 0

    def get_secret(self, name):
        self.calls += 1
        if name not in self._secrets:
            raise KeyError(name)
        return FakeSecret(self._secrets[name])


def test_secret_is_read_from_key_vault_and_cached():
    client = FakeSecretClient({"snowflake-synapse-private-key": "PEM"})
    resolver = SecretResolver(
        key_vault_url="https://kv.vault.azure.net/",
        client_factory=lambda url: client,
        environ={},
    )

    assert resolver.get("snowflake-synapse-private-key") == "PEM"
    assert resolver.get("snowflake-synapse-private-key") == "PEM"
    assert client.calls == 1


def test_environment_fallback_when_no_vault_configured():
    resolver = SecretResolver(environ={"SNOWFLAKE_SYNAPSE_PRIVATE_KEY": "LOCAL_PEM"})
    assert resolver.get("snowflake-synapse-private-key") == "LOCAL_PEM"


def test_missing_secret_raises():
    resolver = SecretResolver(environ={})
    with pytest.raises(KeyError):
        resolver.get("does-not-exist")


def test_snowflake_config_builds_qualified_names():
    config = SnowflakeConfig.from_env("uat")
    assert config.staging_table("STG_CUSTOMER_360").endswith(
        ".STAGING.STG_CUSTOMER_360"
    )
    assert config.staging_table("STG_CUSTOMER_360").startswith("ETL_STAGING_UAT.")
    assert config.data_product_table("CUSTOMER_MASTER_PROFILE") == (
        "DATA_PRODUCTS_UAT.DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE"
    )


def test_pipeline_config_from_env(monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "PROD")
    monkeypatch.setenv("RUN_DATE", "2026-04-10")
    monkeypatch.setenv("PIPELINE_RUN_ID", "abc-123")

    config = PipelineConfig.from_env()

    assert config.environment == "PROD"
    assert config.run_date.isoformat() == "2026-04-10"
    assert config.run_id == "abc-123"
    assert config.run_log_fqn == "DATA_PRODUCTS_PROD.DATA_PRODUCTS.PIPELINE_RUN_LOG"
