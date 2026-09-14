from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..audit import assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..tables import write_overwrite


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    customers = spark.table(cfg.fqn(cfg.bronze_core_schema, "customers")).alias("c")
    addresses = spark.table(cfg.fqn(cfg.bronze_core_schema, "addresses"))
    accounts = spark.table(cfg.fqn(cfg.bronze_core_schema, "accounts"))
    run_date = F.lit(cfg.run_date).cast("date")

    address_window = Window.partitionBy("customer_id").orderBy(F.col("effective_date").desc())
    home = (
        addresses.where(
            (F.col("address_type") == "HOME")
            & (F.col("expiration_date").isNull() | (F.col("expiration_date") > run_date))
        )
        .withColumn("_rn", F.row_number().over(address_window))
        .where(F.col("_rn") == 1)
        .select(
            "customer_id",
            F.concat(
                F.trim("address_line_1"),
                F.when(F.col("address_line_2").isNull(), F.lit("")).otherwise(
                    F.concat(F.lit(", "), F.trim("address_line_2"))
                ),
            ).alias("primary_address"),
            "city",
            "state_code",
            "zip_code",
        )
        .alias("a")
    )
    account_agg = (
        accounts.groupBy("customer_id")
        .agg(
            F.count("*").alias("num_accounts"),
            F.sum(F.when(F.col("account_status") == "O", 1).otherwise(0)).alias(
                "num_active_accounts"
            ),
            F.max(F.when(F.col("account_type") == "CHECKING", "Y").otherwise("N")).alias(
                "has_checking"
            ),
            F.max(F.when(F.col("account_type") == "SAVINGS", "Y").otherwise("N")).alias(
                "has_savings"
            ),
            F.max(F.when(F.col("account_type") == "CREDIT", "Y").otherwise("N")).alias(
                "has_credit"
            ),
            F.max(F.when(F.col("account_type") == "LOAN", "Y").otherwise("N")).alias("has_loan"),
            F.sum(F.coalesce("current_balance", F.lit(0))).alias("total_balance"),
            F.sum(
                F.when(
                    F.col("account_type") == "CREDIT", F.coalesce("credit_limit", F.lit(0))
                ).otherwise(0)
            ).alias("total_credit_limit"),
            F.sum(
                F.when(
                    F.col("account_type") == "CREDIT", F.coalesce("current_balance", F.lit(0))
                ).otherwise(0)
            ).alias("credit_balance"),
        )
        .alias("acct")
    )

    result = (
        customers.join(home, "customer_id", "left")
        .join(account_agg, "customer_id", "left")
        .where(F.col("customer_status").isin("A", "I"))
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "date_of_birth",
            (F.year(run_date) - F.year("date_of_birth")).cast("smallint").alias("age"),
            "customer_since",
            (
                (F.year(run_date) - F.year("customer_since")) * 12
                + F.month(run_date)
                - F.month("customer_since")
            )
            .cast("int")
            .alias("tenure_months"),
            "customer_status",
            "segment_code",
            "branch_id",
            "primary_address",
            "city",
            "state_code",
            "zip_code",
            F.col("num_accounts").cast("smallint").alias("num_accounts"),
            F.col("num_active_accounts").cast("smallint").alias("num_active_accounts"),
            "has_checking",
            "has_savings",
            "has_credit",
            "has_loan",
            F.col("total_balance").cast("decimal(18,2)").alias("total_balance"),
            F.col("total_credit_limit").cast("decimal(18,2)").alias("total_credit_limit"),
            F.when(
                F.col("total_credit_limit") > 0,
                (F.col("credit_balance") / F.col("total_credit_limit") * 100).cast("decimal(5,2)"),
            )
            .otherwise(F.lit(0).cast("decimal(5,2)"))
            .alias("credit_utilization_pct"),
            F.current_timestamp().alias("load_ts"),
        )
    )
    return result


def run(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    fqn = cfg.fqn(cfg.silver_schema, "stg_customer_360")
    with step(spark, cfg, "01_stg_customer_360", "FULL_LOAD") as state:
        result = build(spark, cfg)
        state["row_count"] = assert_rows(result, "stg_customer_360")
        write_overwrite(result, fqn)
    validate_table(spark, fqn, ["customer_id"], ["customer_id"], cfg.dq_min_rows)
    return result
