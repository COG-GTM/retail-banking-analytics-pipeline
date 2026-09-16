from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import Window

from ..date_utils import datediff_month, datediff_year


def build_stg_customer_360(customers, addresses, accounts, run_date: date):
    """Port of bteq/01_stg_customer_360.bteq (phase2 2a)."""
    rd = F.lit(run_date)

    addr_ranked = (addresses
        .filter((F.col("address_type") == "HOME")
                & (F.col("expiration_date").isNull()
                   | (F.col("expiration_date") > rd)))
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("customer_id")
                  .orderBy(F.col("effective_date").desc())))
        .filter(F.col("_rn") == 1)
        .select("customer_id", "address_line_1", "address_line_2",
                "city", "state_code", "zip_code"))

    acct_agg = (accounts.groupBy("customer_id").agg(
        F.count("*").alias("num_accounts"),
        F.sum(F.when(F.col("account_status") == "O", 1).otherwise(0))
            .alias("num_active_accounts"),
        F.max(F.when(F.col("account_type") == "CHECKING", "Y").otherwise("N"))
            .alias("has_checking"),
        F.max(F.when(F.col("account_type") == "SAVINGS", "Y").otherwise("N"))
            .alias("has_savings"),
        F.max(F.when(F.col("account_type") == "CREDIT", "Y").otherwise("N"))
            .alias("has_credit"),
        F.max(F.when(F.col("account_type") == "LOAN", "Y").otherwise("N"))
            .alias("has_loan"),
        F.sum(F.coalesce(F.col("current_balance"), F.lit(0)))
            .alias("total_balance"),
        F.sum(F.when(F.col("account_type") == "CREDIT",
                     F.coalesce(F.col("credit_limit"), F.lit(0)))
                .otherwise(0)).alias("total_credit_limit"),
        F.sum(F.when(F.col("account_type") == "CREDIT",
                     F.coalesce(F.col("current_balance"), F.lit(0)))
                .otherwise(0)).alias("credit_balance"),
    ))

    return (customers
        .filter(F.col("customer_status").isin("A", "I"))
        .join(addr_ranked, "customer_id", "left")
        .join(acct_agg, "customer_id", "left")
        .select(
            "customer_id", "first_name", "last_name", "date_of_birth",
            datediff_year("date_of_birth", rd).cast("smallint").alias("age"),
            "customer_since",
            datediff_month("customer_since", rd).cast("int")
                .alias("tenure_months"),
            "customer_status", "segment_code", "branch_id",
            F.concat("address_line_1",
                     F.coalesce(F.concat(F.lit(", "), "address_line_2"),
                                F.lit(""))).alias("primary_address"),
            "city", "state_code", "zip_code",
            "num_accounts", "num_active_accounts",
            "has_checking", "has_savings", "has_credit", "has_loan",
            "total_balance", "total_credit_limit",
            F.when(F.col("total_credit_limit") > 0,
                   (F.col("credit_balance") / F.col("total_credit_limit") * 100)
                   .cast("decimal(5,2)"))
             .otherwise(F.lit(0.00).cast("decimal(5,2)"))
             .alias("credit_utilization_pct"),
            F.current_timestamp().alias("load_ts"),
        ))
