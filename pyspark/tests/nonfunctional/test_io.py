"""The IO abstraction replaces the legacy LIBNAME macro: jobs never touch paths or credentials."""

from __future__ import annotations

import pytest

from common import schemas
from common.io import InMemoryDataIO, LocalDataIO, TableNotFoundError
from orchestration.sample_data import SOURCE_FILES, sample_source_io

pytestmark = pytest.mark.nonfunctional


def test_sample_source_tables_load_against_their_contract(source_io):
    for spec in (
        schemas.CUSTOMERS,
        schemas.ACCOUNTS,
        schemas.ADDRESSES,
        schemas.TRANSACTION_TYPES,
        schemas.CUSTOMER_BUREAU_SCORES,
    ):
        df = source_io.read_spec(spec)
        assert df.columns == list(spec.column_names)
        assert df.count() > 0


def test_lower_case_csv_headers_are_matched_by_name(source_io):
    customers = source_io.read_spec(schemas.CUSTOMERS)
    row = customers.filter("CUSTOMER_ID = 1").collect()[0]

    assert row["FIRST_NAME"] == "Danielle"
    assert str(row["DATE_OF_BIRTH"]) == "2002-04-24"


def test_every_declared_sample_file_exists(repo_root):
    for relative in SOURCE_FILES.values():
        assert (repo_root / relative).is_file(), relative


def test_missing_table_raises(spark, tmp_path):
    io = LocalDataIO(spark=spark, base_path=tmp_path)
    assert not io.table_exists("CORE_BANKING_DB", "CUSTOMERS")
    with pytest.raises(TableNotFoundError):
        io.read_spec(schemas.CUSTOMERS)


def test_local_round_trip_in_parquet_partitions_per_the_ddl(spark, tmp_path, make_df):
    io = LocalDataIO(spark=spark, base_path=tmp_path, fmt="parquet")
    rows = make_df(
        schemas.TRANSACTION_ANALYTICS,
        [
            {"CUSTOMER_ID": 1, "REPORTING_PERIOD": "2026-04", "TOTAL_ACCOUNTS": 2},
            {"CUSTOMER_ID": 2, "REPORTING_PERIOD": "2026-03", "TOTAL_ACCOUNTS": 1},
        ],
    )

    written = io.write_spec(rows, schemas.TRANSACTION_ANALYTICS)

    assert written == 2
    assert (
        tmp_path / "DATA_PRODUCTS_DB" / "TRANSACTION_ANALYTICS.parquet" / "REPORTING_PERIOD=2026-04"
    ).is_dir()
    assert io.table_exists("DATA_PRODUCTS_DB", "TRANSACTION_ANALYTICS")
    assert io.read_spec(schemas.TRANSACTION_ANALYTICS).count() == 2


def test_local_csv_round_trip_and_append(spark, tmp_path, make_df):
    io = LocalDataIO(spark=spark, base_path=tmp_path, fmt="csv")
    rows = make_df(schemas.ETL_RUN_LOG, [{"JOB_NAME": "j", "STATUS": "SUCCESS", "ROW_COUNT": 1}])

    io.write_spec(rows, schemas.ETL_RUN_LOG)
    io.write_spec(rows, schemas.ETL_RUN_LOG, mode="append")

    assert io.read_spec(schemas.ETL_RUN_LOG).count() == 2


def test_in_memory_io_enforces_the_contract_and_appends(spark, make_df):
    io = InMemoryDataIO()
    rows = make_df(schemas.STG_CUSTOMER_360, [{"CUSTOMER_ID": 1}])

    assert not io.table_exists("ETL_STAGING_DB", "STG_CUSTOMER_360")
    assert io.write_spec(rows, schemas.STG_CUSTOMER_360) == 1
    assert io.write_spec(rows, schemas.STG_CUSTOMER_360, mode="append") == 2
    assert io.read_spec(schemas.STG_CUSTOMER_360).columns == list(
        schemas.STG_CUSTOMER_360.column_names
    )

    with pytest.raises(TableNotFoundError):
        io.read_spec(schemas.STG_RISK_FACTORS)


def test_path_overrides_take_precedence(spark, repo_root):
    io = sample_source_io(spark, repo_root)

    assert io.path_for("CORE_BANKING_DB", "CUSTOMERS").name == "customers.csv"
    assert io.path_for("ETL_STAGING_DB", "STG_CUSTOMER_360").name == "STG_CUSTOMER_360.csv"
