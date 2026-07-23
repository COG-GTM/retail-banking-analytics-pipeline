"""Ticket 4 - Customer 360 staging.

PySpark port of ``bteq/01_stg_customer_360.bteq``.

Inputs  : core_banking.customers, core_banking.accounts, core_banking.addresses
Output  : etl_staging.stg_customer_360  (Delta)
Business logic preserved verbatim:
    * closed customers (status 'C') excluded; 'A'/'I' kept
    * primary address = most recent non-expired HOME address
      (Teradata QUALIFY ROW_NUMBER -> window function)
    * age, tenure, credit-utilization derivations
"""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

OUTPUT_COLUMNS = [
    "customer_id", "first_name", "last_name", "date_of_birth", "age",
    "customer_since", "tenure_months", "customer_status", "segment_code",
    "branch_id", "primary_address", "city", "state_code", "zip_code",
    "num_accounts", "num_active_accounts", "has_checking", "has_savings",
    "has_credit", "has_loan", "total_balance", "total_credit_limit",
    "credit_utilization_pct", "load_ts",
]


def _primary_address(addresses: DataFrame, run_date: date) -> DataFrame:
    """Most recent, non-expired HOME address per customer (QUALIFY ROW_NUMBER)."""
    eligible = addresses.where(
        (F.col("address_type") == "HOME")
        & (F.col("expiration_date").isNull() | (F.col("expiration_date") > F.lit(run_date)))
    )
    # effective_date DESC; address_id DESC as a deterministic tie-breaker.
    w = Window.partitionBy("customer_id").orderBy(
        F.col("effective_date").desc(), F.col("address_id").desc()
    )
    return (
        eligible.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select(
            "customer_id", "address_line_1", "address_line_2",
            "city", "state_code", "zip_code",
        )
    )


def _account_aggregates(accounts: DataFrame) -> DataFrame:
    def has(acct_type: str) -> "F.Column":
        return F.max(F.when(F.col("account_type") == acct_type, "Y").otherwise("N"))

    return accounts.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("num_accounts"),
        F.sum(F.when(F.col("account_status") == "O", 1).otherwise(0)).alias(
            "num_active_accounts"
        ),
        has("CHECKING").alias("has_checking"),
        has("SAVINGS").alias("has_savings"),
        has("CREDIT").alias("has_credit"),
        has("LOAN").alias("has_loan"),
        F.sum(F.coalesce(F.col("current_balance"), F.lit(0))).alias("total_balance"),
        F.sum(
            F.when(F.col("account_type") == "CREDIT", F.coalesce(F.col("credit_limit"), F.lit(0)))
            .otherwise(F.lit(0))
        ).alias("total_credit_limit"),
        F.sum(
            F.when(F.col("account_type") == "CREDIT", F.coalesce(F.col("current_balance"), F.lit(0)))
            .otherwise(F.lit(0))
        ).alias("credit_balance"),
    )


def build_stg_customer_360(
    customers: DataFrame,
    accounts: DataFrame,
    addresses: DataFrame,
    run_date: date,
    load_ts: datetime,
) -> DataFrame:
    addr = _primary_address(addresses, run_date)
    acct = _account_aggregates(accounts)

    primary_address = F.concat(
        F.trim(F.col("address_line_1")),
        F.coalesce(F.concat(F.lit(", "), F.trim(F.col("address_line_2"))), F.lit("")),
    )

    credit_util = (
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("credit_balance") / F.col("total_credit_limit") * 100).cast("decimal(5,2)"),
        )
        .otherwise(F.lit(0.00).cast("decimal(5,2)"))
    )

    return (
        customers.where(F.col("customer_status").isin("A", "I"))
        .join(addr, "customer_id", "left")
        .join(acct, "customer_id", "left")
        .select(
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("date_of_birth"),
            (F.datediff(F.lit(run_date), F.col("date_of_birth")) / F.lit(365.25))
            .cast("smallint").alias("age"),
            F.col("customer_since"),
            F.months_between(F.lit(run_date), F.col("customer_since")).cast("int").alias(
                "tenure_months"
            ),
            F.col("customer_status"),
            F.col("segment_code"),
            F.col("branch_id"),
            primary_address.alias("primary_address"),
            F.col("city"),
            F.col("state_code"),
            F.col("zip_code"),
            F.col("num_accounts").cast("smallint").alias("num_accounts"),
            F.col("num_active_accounts").cast("smallint").alias("num_active_accounts"),
            F.col("has_checking"),
            F.col("has_savings"),
            F.col("has_credit"),
            F.col("has_loan"),
            F.col("total_balance").cast("decimal(18,2)").alias("total_balance"),
            F.col("total_credit_limit").cast("decimal(18,2)").alias("total_credit_limit"),
            credit_util.alias("credit_utilization_pct"),
            F.lit(load_ts).cast("timestamp").alias("load_ts"),
        )
    )
