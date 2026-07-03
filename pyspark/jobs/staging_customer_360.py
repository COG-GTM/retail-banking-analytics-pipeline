"""Staging job 01 -- STG_CUSTOMER_360.

Faithful PySpark port of ``bteq/01_stg_customer_360.bteq``: a denormalised
customer-360 view joining CUSTOMERS with the most-recent non-expired HOME address
and per-customer account-portfolio aggregates.

BTEQ -> PySpark mapping:
* ``QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_date
  DESC) = 1`` -> :class:`Window` + ``row_number`` filter (``address_id`` added as
  a deterministic tiebreaker).
* Age / tenure ``CAST(... AS SMALLINT/INTEGER)`` -> :mod:`common.dates`.
* ``TRIM(a.ADDRESS_LINE_1) || COALESCE(', ' || TRIM(a.ADDRESS_LINE_2), '')`` ->
  ``concat`` (null-propagating, so a missing address yields NULL, matching CTAS).
* ``WHERE CUSTOMER_STATUS IN ('A','I')`` retained verbatim.

This module is the reference implementation other job modules mirror: pure
``transform`` functions (unit-testable on in-memory DataFrames) + a thin
:func:`run` that wires I/O, validation, audit and the schema contract.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.dates import age_expr, load_timestamp, tenure_months_expr
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table

JOB_NAME = "01_stg_customer_360"
TARGET = "STG_CUSTOMER_360"

_ACTIVE_STATUSES = ("A", "I")


def primary_address(addresses: DataFrame, run_date: _dt.date) -> DataFrame:
    """Most-recent, non-expired HOME address per customer (BTEQ subquery ``a``)."""
    win = Window.partitionBy("customer_id").orderBy(
        F.col("effective_date").desc_nulls_last(), F.col("address_id").desc()
    )
    return (
        addresses
        .filter(F.col("address_type") == "HOME")
        .filter(F.col("expiration_date").isNull() | (F.col("expiration_date") > F.lit(run_date)))
        .withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_id", "address_line_1", "address_line_2",
            "city", "state_code", "zip_code",
        )
    )


def account_agg(accounts: DataFrame) -> DataFrame:
    """Per-customer account portfolio metrics (BTEQ subquery ``acct_agg``)."""
    def has(acct_type: str) -> F.Column:
        return F.max(F.when(F.col("account_type") == acct_type, "Y").otherwise("N"))

    credit_amt = lambda col: F.sum(  # noqa: E731
        F.when(F.col("account_type") == "CREDIT", F.coalesce(F.col(col), F.lit(0))).otherwise(F.lit(0))
    )
    return (
        accounts.groupBy("customer_id").agg(
            F.count(F.lit(1)).alias("num_accounts"),
            F.sum(F.when(F.col("account_status") == "O", 1).otherwise(0)).alias("num_active_accounts"),
            has("CHECKING").alias("has_checking"),
            has("SAVINGS").alias("has_savings"),
            has("CREDIT").alias("has_credit"),
            has("LOAN").alias("has_loan"),
            F.sum(F.coalesce(F.col("current_balance"), F.lit(0))).alias("total_balance"),
            credit_amt("credit_limit").alias("total_credit_limit"),
            credit_amt("current_balance").alias("credit_balance"),
        )
    )


def transform(
    customers: DataFrame,
    accounts: DataFrame,
    addresses: DataFrame,
    config: PipelineConfig,
) -> DataFrame:
    """Build STG_CUSTOMER_360 (schema-enforced to the DDL)."""
    run_date = config.run_date
    addr = primary_address(addresses, run_date)
    acct = account_agg(accounts)

    # credit_utilization_pct; final decimal(5,2) cast applied by enforce_schema.
    credit_util = F.when(
        F.col("total_credit_limit") > 0,
        (F.col("credit_balance") / F.col("total_credit_limit") * 100),
    ).otherwise(F.lit(0.00))

    df = (
        customers.alias("c")
        .filter(F.col("customer_status").isin(*_ACTIVE_STATUSES))
        .join(addr.alias("a"), "customer_id", "left")
        .join(acct.alias("acct"), "customer_id", "left")
        .withColumn("age", age_expr("date_of_birth", run_date))
        .withColumn("tenure_months", tenure_months_expr("customer_since", run_date))
        .withColumn(
            "primary_address",
            F.concat(
                F.trim(F.col("address_line_1")),
                F.coalesce(F.concat(F.lit(", "), F.trim(F.col("address_line_2"))), F.lit("")),
            ),
        )
        .withColumn("credit_utilization_pct", credit_util)
        .withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    )
    return schemas.enforce_schema(df, schemas.STG_CUSTOMER_360)


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read sources, transform, validate, and write STG_CUSTOMER_360."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", "Beginning customer-360 staging")

    customers = io.read_source("CUSTOMERS")
    accounts = io.read_source("ACCOUNTS")
    addresses = io.read_source("ADDRESSES")

    out = transform(customers, accounts, addresses, config).cache()
    n = out.count()

    result = validate_table(
        out, TARGET, key_cols=["customer_id"], not_null=["customer_id"], min_rows=1, audit=audit,
    )
    abort_on_failure(result)

    io.write_staging(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Staging table written", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build STG_CUSTOMER_360")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(spark, config, args.source_dir, args.lake_dir)
    run(spark, io, config)


if __name__ == "__main__":
    main()
