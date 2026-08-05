"""PySpark port of ``bteq/01_stg_customer_360.bteq``.

Builds ``ETL_STAGING_DB.STG_CUSTOMER_360``: a denormalised customer-360 row per active or
inactive customer, joining the most recent unexpired HOME address and the customer's account
portfolio aggregates.

This module is the reference implementation for the migration: pure ``transform_*`` functions
over DataFrames, a thin :func:`run`, and a CLI ``main``.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.functions import qualify_row_number, run_date_col, td_months_between, yn
from common.io import DataIO
from common.job import STATUS_SUCCESS, JobResult, job_entry_point
from common.schemas import enforce_schema
from common.validation import abort_on_failure, validate_table

JOB_NAME = "01_stg_customer_360"
STEP_NAME = "FULL_LOAD"
TARGET = schemas.STG_CUSTOMER_360

AGE_DAYS_PER_YEAR = 365.25
ACTIVE_CUSTOMER_STATUSES = ("A", "I")
HOME_ADDRESS_TYPE = "HOME"
OPEN_ACCOUNT_STATUS = "O"


def transform_primary_address(addresses: DataFrame, run_date: date) -> DataFrame:
    """Most recent unexpired HOME address per customer.

    ``QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY EFFECTIVE_DATE DESC) = 1``
    with ``ADDRESS_ID DESC`` appended as a deterministic tiebreaker (the legacy ordering is
    ambiguous when a customer has two addresses with the same effective date).
    """

    current = addresses.filter(
        (F.col("ADDRESS_TYPE") == HOME_ADDRESS_TYPE)
        & (F.col("EXPIRATION_DATE").isNull() | (F.col("EXPIRATION_DATE") > run_date_col(run_date)))
    )
    ranked = qualify_row_number(
        current,
        partition_by=("CUSTOMER_ID",),
        order_by=(F.col("EFFECTIVE_DATE").desc(), F.col("ADDRESS_ID").desc()),
    )
    return ranked.select(
        "CUSTOMER_ID",
        "ADDRESS_LINE_1",
        "ADDRESS_LINE_2",
        "CITY",
        "STATE_CODE",
        "ZIP_CODE",
    )


def transform_account_aggregates(accounts: DataFrame) -> DataFrame:
    """Per-customer account portfolio metrics.

    ``MAX(CASE WHEN ACCOUNT_TYPE = 'X' THEN 'Y' ELSE 'N' END)`` relies on ``'Y' > 'N'``
    lexicographically, which is reproduced with ``max`` over the same string expression.
    """

    def has_product(account_type: str) -> Column:
        return F.max(yn(F.col("ACCOUNT_TYPE") == account_type))

    def credit_sum(column: str) -> Column:
        return F.sum(
            F.when(
                F.col("ACCOUNT_TYPE") == "CREDIT", F.coalesce(F.col(column), F.lit(0))
            ).otherwise(F.lit(0))
        )

    return accounts.groupBy("CUSTOMER_ID").agg(
        F.count(F.lit(1)).alias("NUM_ACCOUNTS"),
        F.sum(F.when(F.col("ACCOUNT_STATUS") == OPEN_ACCOUNT_STATUS, 1).otherwise(0)).alias(
            "NUM_ACTIVE_ACCOUNTS"
        ),
        has_product("CHECKING").alias("HAS_CHECKING"),
        has_product("SAVINGS").alias("HAS_SAVINGS"),
        has_product("CREDIT").alias("HAS_CREDIT"),
        has_product("LOAN").alias("HAS_LOAN"),
        F.sum(F.coalesce(F.col("CURRENT_BALANCE"), F.lit(0))).alias("TOTAL_BALANCE"),
        credit_sum("CREDIT_LIMIT").alias("TOTAL_CREDIT_LIMIT"),
        credit_sum("CURRENT_BALANCE").alias("CREDIT_BALANCE"),
    )


def transform_customer_360(
    customers: DataFrame,
    addresses: DataFrame,
    accounts: DataFrame,
    *,
    run_date: date,
    load_ts: Column | None = None,
) -> DataFrame:
    """Full port of the ``CREATE MULTISET TABLE ETL_STAGING_DB.STG_CUSTOMER_360`` statement."""

    load_ts = F.current_timestamp() if load_ts is None else load_ts
    address = transform_primary_address(addresses, run_date)
    portfolio = transform_account_aggregates(accounts)
    today = run_date_col(run_date)

    address_line_2 = F.trim(F.col("ADDRESS_LINE_2"))
    primary_address = F.concat(
        F.trim(F.col("ADDRESS_LINE_1")),
        F.coalesce(F.concat(F.lit(", "), address_line_2), F.lit("")),
    )

    credit_utilization = (
        F.when(
            F.col("TOTAL_CREDIT_LIMIT") > 0,
            (F.col("CREDIT_BALANCE") / F.col("TOTAL_CREDIT_LIMIT") * F.lit(100)).cast(
                "decimal(5,2)"
            ),
        )
        .otherwise(F.lit(0.00).cast("decimal(5,2)"))
        .alias("CREDIT_UTILIZATION_PCT")
    )

    joined = (
        customers.filter(F.col("CUSTOMER_STATUS").isin(*ACTIVE_CUSTOMER_STATUSES))
        .join(address, on="CUSTOMER_ID", how="left")
        .join(portfolio, on="CUSTOMER_ID", how="left")
    )

    projected = joined.select(
        F.col("CUSTOMER_ID"),
        F.col("FIRST_NAME"),
        F.col("LAST_NAME"),
        F.col("DATE_OF_BIRTH"),
        (F.datediff(today, F.col("DATE_OF_BIRTH")) / F.lit(AGE_DAYS_PER_YEAR))
        .cast("smallint")
        .alias("AGE"),
        F.col("CUSTOMER_SINCE"),
        td_months_between(today, F.col("CUSTOMER_SINCE")).alias("TENURE_MONTHS"),
        F.col("CUSTOMER_STATUS"),
        F.col("SEGMENT_CODE"),
        F.col("BRANCH_ID"),
        primary_address.alias("PRIMARY_ADDRESS"),
        F.col("CITY"),
        F.col("STATE_CODE"),
        F.col("ZIP_CODE"),
        F.col("NUM_ACCOUNTS"),
        F.col("NUM_ACTIVE_ACCOUNTS"),
        F.col("HAS_CHECKING"),
        F.col("HAS_SAVINGS"),
        F.col("HAS_CREDIT"),
        F.col("HAS_LOAN"),
        F.col("TOTAL_BALANCE"),
        F.col("TOTAL_CREDIT_LIMIT"),
        credit_utilization,
        load_ts.alias("LOAD_TS"),
    )
    return enforce_schema(projected, TARGET)


def run(spark: SparkSession, io: DataIO, config: PipelineConfig, audit: AuditLog) -> JobResult:
    """Execute the job end to end, mirroring the BTEQ script's step sequence."""

    result = JobResult(job_name=JOB_NAME, target_table=TARGET.qualified_name)
    audit.log_step(JOB_NAME, "START", "Building STG_CUSTOMER_360")

    customers = io.read_spec(schemas.CUSTOMERS)
    addresses = io.read_spec(schemas.ADDRESSES)
    accounts = io.read_spec(schemas.ACCOUNTS)

    output = transform_customer_360(
        customers, addresses, accounts, run_date=config.run_date
    ).persist()

    validation = validate_table(
        output,
        table=TARGET.qualified_name,
        key_cols=TARGET.primary_index,
        not_null=("CUSTOMER_ID",),
        min_rows=1,
    )
    result.validation = validation
    abort_on_failure(validation)

    # Step 2 in the BTEQ script; Spark maintains its own statistics.
    audit.log_step(JOB_NAME, "START", "COLLECT STATISTICS (no-op on Spark)")

    row_count = io.write_spec(output, TARGET)
    output.unpersist()

    result.row_count = row_count
    result.status = STATUS_SUCCESS
    result.end_ts = datetime.now()
    audit.log_step(JOB_NAME, "SUCCESS", "STG_CUSTOMER_360 loaded", rowcount=row_count)
    audit.log_run(JOB_NAME, STEP_NAME, "SUCCESS", row_count, result.start_ts, result.end_ts)
    return result


main = job_entry_point(run, JOB_NAME)


if __name__ == "__main__":
    main()
