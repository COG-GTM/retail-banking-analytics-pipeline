import pytest

from pipeline_utils.config import SnowflakeConfig
from pipeline_utils.secrets import SecretResolver
from pipeline_utils.snowflake_io import SNOWFLAKE_SPARK_SOURCE, SnowflakeIO


class FakeWriter:
    def __init__(self, recorder):
        self.recorder = recorder

    def format(self, source):
        self.recorder["format"] = source
        return self

    def options(self, **kwargs):
        self.recorder.setdefault("options", {}).update(kwargs)
        return self

    def option(self, key, value):
        self.recorder.setdefault("options", {})[key] = value
        return self

    def mode(self, mode):
        self.recorder["mode"] = mode
        return self

    def save(self):
        self.recorder["saved"] = True

    def load(self):
        self.recorder["loaded"] = True
        return "dataframe"


class FakeDataFrame:
    def __init__(self, recorder):
        self.write = FakeWriter(recorder)


class FakeSpark:
    def __init__(self, recorder):
        self.read = FakeWriter(recorder)


@pytest.fixture
def io_and_recorder():
    config = SnowflakeConfig(
        account="acme-eu",
        user="SVC_SYNAPSE_SPARK",
        role="TRANSFORMER",
        warehouse="WH_SPARK",
        staging_database="ETL_STAGING_DEV",
        staging_schema="STAGING",
        data_products_database="DATA_PRODUCTS_DEV",
        data_products_schema="DATA_PRODUCTS",
        private_key_secret_name="snowflake-synapse-private-key",
        key_vault_url="",
    )
    resolver = SecretResolver(environ={"SNOWFLAKE_SYNAPSE_PRIVATE_KEY": "PEM"})
    recorder = {}
    return SnowflakeIO(config, resolver), recorder


def test_read_table_uses_the_spark_connector(io_and_recorder):
    io, recorder = io_and_recorder

    result = io.read_table(
        FakeSpark(recorder), "ETL_STAGING_DEV.STAGING.STG_CUSTOMER_360"
    )

    assert result == "dataframe"
    assert recorder["format"] == SNOWFLAKE_SPARK_SOURCE
    assert recorder["options"]["dbtable"] == "STG_CUSTOMER_360"
    assert recorder["options"]["sfDatabase"] == "ETL_STAGING_DEV"
    assert recorder["options"]["sfSchema"] == "STAGING"
    assert recorder["options"]["pem_private_key"] == "PEM"


def test_write_table_truncates_instead_of_replacing(io_and_recorder):
    io, recorder = io_and_recorder

    io.write_table(
        FakeDataFrame(recorder), "DATA_PRODUCTS_DEV.DATA_PRODUCTS.CUSTOMER_MASTER_PROFILE"
    )

    assert recorder["mode"] == "overwrite"
    assert recorder["options"]["truncate_table"] == "on"
    assert recorder["options"]["dbtable"] == "CUSTOMER_MASTER_PROFILE"
    assert recorder["saved"] is True


def test_base_options_never_contain_the_private_key(io_and_recorder):
    io, _ = io_and_recorder
    options = io.base_options("DATA_PRODUCTS_DEV", "DATA_PRODUCTS")
    assert "pem_private_key" not in options
    assert options["sfURL"] == "acme-eu.snowflakecomputing.com"


def test_unqualified_table_name_is_rejected(io_and_recorder):
    io, recorder = io_and_recorder
    with pytest.raises(ValueError):
        io.read_table(FakeSpark(recorder), "CUSTOMER_MASTER_PROFILE")
