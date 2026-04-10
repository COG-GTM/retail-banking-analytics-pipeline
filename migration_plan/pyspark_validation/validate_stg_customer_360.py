"""
PySpark Validation: STG_CUSTOMER_360
Replaces: bteq/01_stg_customer_360.bteq

Validates that the PySpark translation of the BTEQ customer-360 staging
script produces output matching the reference CSV data.
"""
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    count,
    current_date,
    current_timestamp,
    datediff,
    desc,
    floor,
    lit,
    max as spark_max,
    months_between,
    row_number,
    sum as spark_sum,
    trim,
    when,
)
from pyspark.sql.types import IntegerType

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
SOURCE_DIR = DATA_DIR / "01_source_tables"
REFERENCE_DIR = DATA_DIR / "02_bteq_staging"


def build_stg_customer_360(spark: SparkSession):
    """Replicate the logic of 01_stg_customer_360.bteq in PySpark."""

    # ------------------------------------------------------------------
    # 1. Load source tables
    # ------------------------------------------------------------------
    customers = spark.read.csv(
        str(SOURCE_DIR / "customers.csv"), header=True, inferSchema=True
    )
    accounts = spark.read.csv(
        str(SOURCE_DIR / "accounts.csv"), header=True, inferSchema=True
    )
    addresses = spark.read.csv(
        str(SOURCE_DIR / "addresses.csv"), header=True, inferSchema=True
    )

    # ------------------------------------------------------------------
    # 2. Primary address: latest HOME address not expired
    #    BTEQ: QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id
    #           ORDER BY effective_date DESC) = 1
    # ------------------------------------------------------------------
    addr_window = Window.partitionBy("customer_id").orderBy(desc("effective_date"))
    primary_addr = (
        addresses.filter(
            (col("address_type") == "HOME")
            & (col("expiration_date").isNull() | (col("expiration_date") > current_date()))
        )
        .withColumn("rn", row_number().over(addr_window))
        .filter(col("rn") == 1)
        .select(
            col("customer_id").alias("addr_cust_id"),
            concat(
                trim(col("address_line_1")),
                coalesce(concat(lit(", "), trim(col("address_line_2"))), lit(""))
            ).alias("primary_address"),
            "city",
            "state_code",
            "zip_code",
        )
    )

    # ------------------------------------------------------------------
    # 3. Account aggregations per customer
    # ------------------------------------------------------------------
    acct_agg = accounts.groupBy("customer_id").agg(
        count("*").alias("num_accounts"),
        spark_sum(when(col("account_status") == "O", 1).otherwise(0)).alias(
            "num_active_accounts"
        ),
        spark_max(
            when(col("account_type") == "CHECKING", lit("Y")).otherwise(lit("N"))
        ).alias("has_checking"),
        spark_max(
            when(col("account_type") == "SAVINGS", lit("Y")).otherwise(lit("N"))
        ).alias("has_savings"),
        spark_max(
            when(col("account_type") == "CREDIT", lit("Y")).otherwise(lit("N"))
        ).alias("has_credit"),
        spark_max(
            when(col("account_type") == "LOAN", lit("Y")).otherwise(lit("N"))
        ).alias("has_loan"),
        spark_sum(coalesce(col("current_balance"), lit(0))).alias("total_balance"),
        spark_sum(
            when(col("account_type") == "CREDIT", coalesce(col("credit_limit"), lit(0))).otherwise(0)
        ).alias("total_credit_limit"),
        spark_sum(
            when(col("account_type") == "CREDIT", coalesce(col("current_balance"), lit(0))).otherwise(0)
        ).alias("credit_balance"),
    )

    # Credit utilization = credit_balance / total_credit_limit * 100
    # Matches BTEQ: CREDIT_BALANCE / TOTAL_CREDIT_LIMIT * 100
    acct_agg = acct_agg.withColumn(
        "credit_utilization_pct",
        when(
            col("total_credit_limit") > 0,
            col("credit_balance") / col("total_credit_limit") * 100,
        ).otherwise(0.0),
    )

    # ------------------------------------------------------------------
    # 4. Join and derive
    # ------------------------------------------------------------------
    result = (
        customers.filter(col("customer_status").isin("A", "I"))
        .join(primary_addr, customers["customer_id"] == primary_addr["addr_cust_id"], "left")
        .join(acct_agg, "customer_id", "left")
        .withColumn(
            "age",
            floor(datediff(current_date(), col("date_of_birth")) / 365.25).cast(
                IntegerType()
            ),
        )
        .withColumn(
            "tenure_months",
            months_between(current_date(), col("customer_since")).cast(IntegerType()),
        )
        .withColumn("load_ts", current_timestamp())
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "date_of_birth",
            "age",
            "customer_since",
            "tenure_months",
            "customer_status",
            "segment_code",
            "branch_id",
            "primary_address",
            "city",
            "state_code",
            "zip_code",
            "num_accounts",
            "num_active_accounts",
            "has_checking",
            "has_savings",
            "has_credit",
            "has_loan",
            "total_balance",
            "total_credit_limit",
            "credit_utilization_pct",
            "load_ts",
        )
    )

    return result


def validate(spark: SparkSession) -> dict:
    """Run the transformation and compare to reference data."""
    result = build_stg_customer_360(spark)
    reference = spark.read.csv(
        str(REFERENCE_DIR / "stg_customer_360.csv"), header=True, inferSchema=True
    )

    checks = {}

    # Row count
    result_count = result.count()
    ref_count = reference.count()
    checks["row_count_match"] = result_count == ref_count
    checks["row_count_pyspark"] = result_count
    checks["row_count_reference"] = ref_count

    # Column count
    checks["column_count_match"] = len(result.columns) == len(reference.columns)
    checks["columns_pyspark"] = sorted(result.columns)
    checks["columns_reference"] = sorted(reference.columns)

    # Join on customer_id and compare key columns
    joined = result.alias("py").join(
        reference.alias("ref"), col("py.customer_id") == col("ref.customer_id"), "inner"
    )
    join_count = joined.count()
    checks["join_count"] = join_count
    checks["customer_id_overlap_pct"] = (
        round(join_count / max(ref_count, 1) * 100, 2)
    )

    # Numeric comparison on total_balance (tolerance 0.01)
    if join_count > 0:
        balance_diff = joined.withColumn(
            "bal_diff",
            abs(
                coalesce(col("py.total_balance"), lit(0))
                - coalesce(col("ref.total_balance"), lit(0))
            ),
        )
        max_diff = balance_diff.agg({"bal_diff": "max"}).collect()[0][0]
        checks["max_total_balance_diff"] = float(max_diff) if max_diff else 0.0
        checks["total_balance_within_tolerance"] = (
            checks["max_total_balance_diff"] <= 0.01
        )

    checks["overall_pass"] = (
        checks["row_count_match"]
        and checks["column_count_match"]
        and checks.get("total_balance_within_tolerance", False)
    )

    return checks


def abs(col_expr):
    """Absolute value helper."""
    from pyspark.sql.functions import abs as spark_abs
    return spark_abs(col_expr)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("validate_stg_customer_360")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("  Validating: STG_CUSTOMER_360 (BTEQ Script 01)")
    print("=" * 70)

    results = validate(spark)
    for k, v in results.items():
        status = "PASS" if v is True else ("FAIL" if v is False else str(v))
        print(f"  {k:40s} : {status}")

    print("=" * 70)
    overall = "PASS" if results["overall_pass"] else "FAIL"
    print(f"  Overall: {overall}")
    print("=" * 70)

    spark.stop()
