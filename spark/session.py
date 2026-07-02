"""Spark session bootstrap and the data-access layer.

:class:`DataLayer` is the PySpark replacement for the ``connect_teradata`` SAS
macro. Where the macro established SAS/ACCESS ``LIBNAME`` connections (with
``bulkload``/``fastload``) to CORE_BANKING_DB / TXN_PROCESSING_DB /
ETL_STAGING_DB / DATA_PRODUCTS_DB, this layer reads the BTEQ staging datasets
and reads/writes the certified data-product datasets via Spark.

There is no Spark equivalent for Teradata bulk-load / fastload options, so those
are simply dropped -- Spark reads and writes the corresponding datasets directly.
"""
from __future__ import annotations

import glob
import os
import shutil
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from .config import PipelineConfig

# ---------------------------------------------------------------------------
# Explicit schemas for the BTEQ staging inputs (mirror ddl/01_staging_tables.sql).
# Numeric measures/counts are typed DoubleType so the reader tolerates both
# integer ("6") and decimal ("6.0") representations without silent null coercion;
# integer-typed output columns are cast explicitly inside each job. Dates and
# load_ts are kept as strings (their string form is the downstream contract).
# ---------------------------------------------------------------------------
def _num(name: str) -> StructField:
    return StructField(name, DoubleType(), True)


def _str(name: str) -> StructField:
    return StructField(name, StringType(), True)


STG_CUSTOMER_360_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    _str("first_name"), _str("last_name"), _str("date_of_birth"),
    _num("age"), _str("customer_since"), _num("tenure_months"),
    _str("customer_status"), _str("segment_code"), _num("branch_id"),
    _str("primary_address"), _str("city"), _str("state_code"), _str("zip_code"),
    _num("num_accounts"), _num("num_active_accounts"),
    _str("has_checking"), _str("has_savings"), _str("has_credit"), _str("has_loan"),
    _num("total_balance"), _num("total_credit_limit"), _num("credit_utilization_pct"),
    _str("load_ts"),
])

STG_TXN_SUMMARY_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    StructField("account_id", LongType(), False),
    _str("account_type"), _str("summary_period_start"), _str("summary_period_end"),
    _num("txn_count_total"), _num("txn_count_debit"), _num("txn_count_credit"),
    _num("txn_count_fee"), _num("amt_total_debit"), _num("amt_total_credit"),
    _num("amt_total_fees"), _num("amt_avg_debit"), _num("amt_avg_credit"),
    _num("amt_max_single_debit"), _num("amt_max_single_credit"),
    _num("distinct_merchants"), _str("top_merchant_category"),
    _num("pct_atm"), _num("pct_pos"), _num("pct_web"), _num("pct_mobile"),
    _num("days_since_last_txn"), _str("load_ts"),
])

STG_RISK_FACTORS_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    _num("account_overdraft_cnt"), _num("nsf_fee_total"), _num("large_withdrawal_cnt"),
    _num("large_withdrawal_amt"), _num("avg_daily_balance_30d"), _num("avg_daily_balance_90d"),
    _num("balance_volatility"), _num("credit_util_ratio"), _num("payment_ontime_pct"),
    _num("payment_late_cnt"), _num("months_since_last_late"), _num("external_credit_score"),
    _num("debit_velocity_7d"), _num("debit_velocity_30d"), _num("new_merchant_cnt_30d"),
    _num("international_txn_cnt"), _num("high_risk_merchant_cnt"), _str("load_ts"),
])

STAGING_SCHEMAS = {
    "stg_customer_360": STG_CUSTOMER_360_SCHEMA,
    "stg_txn_summary": STG_TXN_SUMMARY_SCHEMA,
    "stg_risk_factors": STG_RISK_FACTORS_SCHEMA,
}


def get_spark(config: PipelineConfig) -> SparkSession:
    """Create (or fetch) the shared Spark session for a run."""
    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )
    for key, value in config.spark_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


class DataLayer:
    """Read staging datasets and read/write data-product datasets.

    Replaces the four ``LIBNAME`` references from ``connect_teradata``:
    ``STGDB`` -> :meth:`read_staging`, ``DPDB`` -> :meth:`read_product` /
    :meth:`write_product`.
    """

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def _path(self, base: str, name: str) -> str:
        return os.path.join(base, f"{name}.{self.config.data_format}")

    def read_staging(self, name: str) -> DataFrame:
        path = self._path(self.config.staging_path, name)
        reader = self.spark.read
        schema = STAGING_SCHEMAS.get(name)
        if self.config.data_format == "csv":
            reader = reader.option("header", True)
            if schema is not None:
                reader = reader.schema(schema)
            else:
                reader = reader.option("inferSchema", True)
            return reader.csv(path)
        return reader.format(self.config.data_format).load(path)

    def read_product(self, name: str) -> DataFrame:
        path = self._path(self.config.products_path, name)
        if self.config.data_format == "csv":
            return (
                self.spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(path)
            )
        return self.spark.read.format(self.config.data_format).load(path)

    def write_product(self, df: DataFrame, name: str) -> str:
        """Write a data-product dataset.

        For CSV output the single-file contract expected by downstream
        consumers (``<name>.csv``) is preserved by coalescing to one partition
        and moving the Spark part-file into place. For non-CSV formats the data
        is written as a directory in the requested format.
        """
        path = self._path(self.config.products_path, name)
        if self.config.data_format == "csv":
            _write_single_csv(df, path)
            return path
        df.write.mode("overwrite").format(self.config.data_format).save(path)
        return path


def _write_single_csv(df: DataFrame, target_path: str) -> None:
    """Write ``df`` to a single CSV file at ``target_path`` (local filesystem)."""
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix="spark_csv_", dir=os.path.dirname(target_path))
    try:
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .option("emptyValue", "")
            .option("nullValue", "")
            .csv(tmp_dir)
        )
        part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        if not part_files:
            raise RuntimeError(f"No CSV part file produced for {target_path}")
        shutil.move(part_files[0], target_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
