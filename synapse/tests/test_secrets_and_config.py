import pytest

from pipeline_utils.config import load_config
from pipeline_utils.secrets import KeyVaultSecretResolver, MappingSecretResolver

BASE_ENV = {
    "SNOWFLAKE_ACCOUNT": "acme-eu",
    "SNOWFLAKE_USER": "SVC_SYNAPSE",
    "AZURE_KEY_VAULT_URL": "https://kv-retail-banking.vault.azure.net/",
}


class FakeVaultClient:
    def __init__(self, values):
        self.values = values
        self.calls = 0

    def get_secret(self, name):
        self.calls += 1
        return type("Secret", (), {"value": self.values[name]})()


def test_load_config_defaults():
    config = load_config({**BASE_ENV, "PIPELINE_ENV": "uat"})
    assert config.env == "UAT"
    assert config.snowflake.database == "RETAIL_BANKING_UAT"
    assert (
        config.qualified(config.snowflake.data_products_schema, "CUSTOMER_MASTER_PROFILE")
        == "RETAIL_BANKING_UAT.DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE"
    )
    assert config.run_log_fqn == "RETAIL_BANKING_UAT.ETL_STAGING.PIPELINE_RUN_LOG"


def test_load_config_requires_key_vault_url():
    env = {k: v for k, v in BASE_ENV.items() if k != "AZURE_KEY_VAULT_URL"}
    with pytest.raises(KeyError, match="AZURE_KEY_VAULT_URL"):
        load_config(env)


def test_config_holds_no_credential_material():
    config = load_config(BASE_ENV)
    assert config.snowflake.private_key_secret == "snowflake-synapse-private-key"
    assert "password" not in repr(config).lower()


def test_key_vault_resolver_caches_lookups():
    client = FakeVaultClient({"snowflake-synapse-private-key": "PEM"})
    resolver = KeyVaultSecretResolver("https://kv.vault.azure.net/", client=client)
    assert resolver.get_secret("snowflake-synapse-private-key") == "PEM"
    assert resolver.get_secret("snowflake-synapse-private-key") == "PEM"
    assert client.calls == 1


def test_mapping_resolver_reports_missing_secret():
    resolver = MappingSecretResolver({"a": "1"})
    assert resolver.get_secret("a") == "1"
    with pytest.raises(KeyError, match="'b'"):
        resolver.get_secret("b")
