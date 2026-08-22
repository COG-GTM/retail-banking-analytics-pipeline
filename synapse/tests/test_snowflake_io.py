from pipeline_utils.config import load_config
from pipeline_utils.secrets import MappingSecretResolver
from pipeline_utils.snowflake_io import SNOWFLAKE_SOURCE, SnowflakeIO

ENV = {
    "SNOWFLAKE_ACCOUNT": "acme-eu",
    "SNOWFLAKE_USER": "SVC_SYNAPSE",
    "AZURE_KEY_VAULT_URL": "https://kv.vault.azure.net/",
    "PIPELINE_ENV": "DEV",
}


class FakeReaderWriter:
    def __init__(self, recorder):
        self.recorder = recorder

    def format(self, source):
        self.recorder["format"] = source
        return self

    def options(self, **kwargs):
        self.recorder["options"] = kwargs
        return self

    def option(self, key, value):
        self.recorder.setdefault("single_options", {})[key] = value
        return self

    def mode(self, mode):
        self.recorder["mode"] = mode
        return self

    def load(self):
        self.recorder["action"] = "load"
        return "dataframe"

    def save(self):
        self.recorder["action"] = "save"


class FakeSpark:
    def __init__(self, recorder):
        self.read = FakeReaderWriter(recorder)


class FakeDataFrame:
    def __init__(self, recorder):
        self.write = FakeReaderWriter(recorder)


def make_io(recorder):
    config = load_config(ENV)
    resolver = MappingSecretResolver({"snowflake-synapse-private-key": "PEM-BODY"})
    return SnowflakeIO(FakeSpark(recorder), config, resolver), config


def test_options_carry_key_pair_credential_from_vault():
    io, _ = make_io({})
    options = io.options("DATA_PRODUCTS")
    assert options["sfURL"] == "acme-eu.snowflakecomputing.com"
    assert options["sfDatabase"] == "RETAIL_BANKING_DEV"
    assert options["sfSchema"] == "DATA_PRODUCTS"
    assert options["pem_private_key"] == "PEM-BODY"
    assert "sfPassword" not in options


def test_read_table_uses_snowflake_source():
    recorder = {}
    io, _ = make_io(recorder)
    assert io.read_table("ETL_STAGING", "STG_CUSTOMER_360") == "dataframe"
    assert recorder["format"] == SNOWFLAKE_SOURCE
    assert recorder["single_options"]["dbtable"] == "STG_CUSTOMER_360"
    assert recorder["action"] == "load"


def test_overwrite_table_truncates_instead_of_recreating():
    recorder = {}
    io, _ = make_io({})
    io.overwrite_table(
        FakeDataFrame(recorder), "DATA_PRODUCTS", "CUSTOMER_MASTER_PROFILE"
    )
    assert recorder["mode"] == "overwrite"
    assert recorder["single_options"]["truncate_table"] == "on"
    assert recorder["action"] == "save"


def test_append_table_appends():
    recorder = {}
    io, _ = make_io({})
    io.append_table(FakeDataFrame(recorder), "ETL_STAGING", "PIPELINE_RUN_LOG")
    assert recorder["mode"] == "append"
    assert recorder["single_options"]["dbtable"] == "PIPELINE_RUN_LOG"
