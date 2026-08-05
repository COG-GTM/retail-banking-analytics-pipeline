"""Deterministic synthetic source data for the performance and e2e tiers.

Every table is built from ``spark.range`` plus column expressions, so generating 10M customers
costs one Spark job and no driver-side Python. The ratios mirror the committed sample extract
(~2.5 accounts per customer, ~64 transactions per account) so a scaled run exercises the same
join fan-out as the real data.

The values are synthetic by construction — no name, address, SSN or account identifier here
derives from a real record.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.io import DataIO
from common.schemas import TableSpec, enforce_schema

ACCOUNTS_PER_CUSTOMER = 2.5
TRANSACTIONS_PER_ACCOUNT = 64

MERCHANT_CATEGORIES = (
    "GROCERY",
    "RESTAURANT",
    "FUEL",
    "RETAIL",
    "TRAVEL",
    "UTILITIES",
    "HEALTHCARE",
    "GAMBLING",
    "CRYPTO",
    "CASH_ADVANCE",
)
CHANNELS = ("ATM", "POS", "WEB", "MOB", "ACH", "WIRE")
ACCOUNT_TYPES = ("CHECKING", "SAVINGS", "CREDIT", "LOAN")
TRANSACTION_TYPE_CODES = ("DEP", "WTH", "POS", "ACH", "FEE", "INT", "TFR", "REV")


def _pick(values: tuple[str, ...], index_expr: str) -> F.Column:
    """Deterministically choose from ``values`` — the generator equivalent of a lookup table."""

    return F.element_at(
        F.array(*[F.lit(value) for value in values]), F.expr(index_expr).cast("int")
    )


def customers(spark: SparkSession, n: int, *, run_date: date) -> DataFrame:
    df = (
        spark.range(n)
        .withColumn("CUSTOMER_ID", (F.col("id") + 1).cast("long"))
        .withColumn("FIRST_NAME", F.concat(F.lit("GIVEN"), F.col("id") % 997))
        .withColumn("LAST_NAME", F.concat(F.lit("FAMILY"), F.col("id") % 1493))
        .withColumn(
            "DATE_OF_BIRTH",
            F.date_sub(F.lit(run_date), (F.lit(6570) + (F.col("id") * 37) % 14600).cast("int")),
        )
        .withColumn("SSN_HASH", F.sha2(F.concat(F.lit("synthetic-"), F.col("id")), 256))
        .withColumn("EMAIL", F.concat(F.lit("customer"), F.col("id"), F.lit("@example.invalid")))
        .withColumn(
            "PHONE_PRIMARY", F.concat(F.lit("555"), F.lpad((F.col("id") % 10000000), 7, "0"))
        )
        .withColumn(
            "CUSTOMER_SINCE",
            F.date_sub(F.lit(run_date), (F.lit(45) + (F.col("id") * 13) % 7000).cast("int")),
        )
        .withColumn(
            "CUSTOMER_STATUS",
            F.when(F.col("id") % 50 == 0, F.lit("C"))
            .when(F.col("id") % 11 == 0, F.lit("I"))
            .otherwise(F.lit("A")),
        )
        .withColumn("SEGMENT_CODE", _pick(("MASS", "AFFLUENT", "PRIVATE"), "pmod(id, 3) + 1"))
        .withColumn("BRANCH_ID", ((F.col("id") % 120) + 1).cast("int"))
        .withColumn("CREATED_TS", F.lit(run_date).cast("timestamp"))
        .withColumn("UPDATED_TS", F.lit(run_date).cast("timestamp"))
    )
    return enforce_schema(df, schemas.CUSTOMERS)


def addresses(spark: SparkSession, n_customers: int, *, run_date: date) -> DataFrame:
    """One HOME and one MAIL address per customer, the HOME one primary and unexpired."""

    df = (
        spark.range(n_customers * 2)
        .withColumn("ADDRESS_ID", (F.col("id") + 1).cast("long"))
        .withColumn("CUSTOMER_ID", ((F.col("id") % n_customers) + 1).cast("long"))
        .withColumn(
            "ADDRESS_TYPE",
            F.when(F.col("id") < n_customers, F.lit("HOME")).otherwise(F.lit("MAIL")),
        )
        .withColumn("ADDRESS_LINE_1", F.concat((F.col("id") % 9000) + 100, F.lit(" SYNTHETIC AVE")))
        .withColumn("ADDRESS_LINE_2", F.lit(None).cast("string"))
        .withColumn(
            "CITY", _pick(("SPRINGFIELD", "RIVERTON", "FAIRVIEW", "LAKESIDE"), "pmod(id, 4) + 1")
        )
        .withColumn("STATE_CODE", _pick(("NY", "CA", "TX", "IL", "FL"), "pmod(id, 5) + 1"))
        .withColumn("ZIP_CODE", F.lpad((F.col("id") % 99999), 5, "0"))
        .withColumn("COUNTRY_CODE", F.lit("US"))
        .withColumn(
            "IS_PRIMARY", F.when(F.col("ADDRESS_TYPE") == "HOME", F.lit("Y")).otherwise(F.lit("N"))
        )
        .withColumn(
            "EFFECTIVE_DATE",
            F.date_sub(F.lit(run_date), (F.lit(30) + (F.col("id") * 7) % 3000).cast("int")),
        )
        .withColumn("EXPIRATION_DATE", F.lit(None).cast("date"))
        .withColumn("CREATED_TS", F.lit(run_date).cast("timestamp"))
        .withColumn("UPDATED_TS", F.lit(run_date).cast("timestamp"))
    )
    return enforce_schema(df, schemas.ADDRESSES)


def accounts(spark: SparkSession, n_customers: int, *, run_date: date) -> DataFrame:
    total = int(n_customers * ACCOUNTS_PER_CUSTOMER)
    df = (
        spark.range(total)
        .withColumn("ACCOUNT_ID", (F.col("id") + 1).cast("long"))
        .withColumn("CUSTOMER_ID", ((F.col("id") % n_customers) + 1).cast("long"))
        .withColumn("ACCOUNT_TYPE", _pick(ACCOUNT_TYPES, "pmod(id, 4) + 1"))
        .withColumn(
            "ACCOUNT_STATUS",
            F.when(F.col("id") % 37 == 0, F.lit("C"))
            .when(F.col("id") % 53 == 0, F.lit("F"))
            .otherwise(F.lit("O")),
        )
        .withColumn(
            "OPEN_DATE",
            F.date_sub(F.lit(run_date), (F.lit(60) + (F.col("id") * 11) % 5000).cast("int")),
        )
        .withColumn(
            "CLOSE_DATE",
            F.when(F.col("id") % 37 == 0, F.date_sub(F.lit(run_date), 15)).otherwise(
                F.lit(None).cast("date")
            ),
        )
        .withColumn(
            "CURRENT_BALANCE", (((F.col("id") * 977) % 250000) / 10.0).cast("decimal(15,2)")
        )
        .withColumn(
            "AVAILABLE_BALANCE", (F.col("CURRENT_BALANCE") * F.lit(0.95)).cast("decimal(15,2)")
        )
        .withColumn(
            "CREDIT_LIMIT",
            F.when(
                F.col("ACCOUNT_TYPE") == "CREDIT",
                (F.lit(1000) + (F.col("id") * 131) % 40000).cast("decimal(15,2)"),
            ).otherwise(F.lit(0).cast("decimal(15,2)")),
        )
        .withColumn("INTEREST_RATE", (((F.col("id") % 1500) + 50) / 10000.0).cast("decimal(5,4)"))
        .withColumn("BRANCH_ID", ((F.col("id") % 120) + 1).cast("int"))
        .withColumn("CREATED_TS", F.lit(run_date).cast("timestamp"))
        .withColumn("UPDATED_TS", F.lit(run_date).cast("timestamp"))
    )
    return enforce_schema(df, schemas.ACCOUNTS)


def bureau_scores(spark: SparkSession, n_customers: int, *, run_date: date) -> DataFrame:
    """A bureau score for ~90% of customers, so the imputation path is exercised."""

    df = (
        spark.range(n_customers)
        .filter(F.col("id") % 10 != 0)
        .withColumn("CUSTOMER_ID", (F.col("id") + 1).cast("long"))
        .withColumn("EXTERNAL_CREDIT_SCORE", (F.lit(300) + (F.col("id") * 53) % 550).cast("int"))
        .withColumn(
            "REPORT_DATE", F.date_sub(F.lit(run_date), ((F.col("id") * 3) % 90).cast("int"))
        )
    )
    return enforce_schema(df, schemas.CUSTOMER_BUREAU_SCORES)


def transaction_types(spark: SparkSession, *, run_date: date) -> DataFrame:
    rows = [
        ("DEP", "Deposit", "CREDIT", "N"),
        ("WTH", "Withdrawal", "DEBIT", "N"),
        ("POS", "Card purchase", "DEBIT", "N"),
        ("ACH", "ACH transfer", "DEBIT", "N"),
        ("FEE", "Service fee", "FEE", "Y"),
        ("INT", "Interest", "INTEREST", "Y"),
        ("TFR", "Internal transfer", "CREDIT", "N"),
        ("REV", "Reversal", "CREDIT", "N"),
    ]
    df = spark.createDataFrame(
        rows, "TRANSACTION_TYPE_CD string, DESCRIPTION string, CATEGORY string, IS_REVENUE string"
    )
    df = df.withColumn("EFFECTIVE_DATE", F.date_sub(F.lit(run_date), 3650)).withColumn(
        "EXPIRATION_DATE", F.lit(None).cast("date")
    )
    return enforce_schema(df, schemas.TRANSACTION_TYPES)


def transactions(spark: SparkSession, n_customers: int, *, run_date: date) -> DataFrame:
    n_accounts = int(n_customers * ACCOUNTS_PER_CUSTOMER)
    total = n_accounts * TRANSACTIONS_PER_ACCOUNT
    df = (
        spark.range(total)
        .withColumn("TRANSACTION_ID", (F.col("id") + 1).cast("long"))
        # every 1000th transaction lands on account 1, giving the ACCOUNT_ID join a hot key so
        # the performance tier can observe AQE's skew handling rather than assume it
        .withColumn(
            "ACCOUNT_ID",
            F.when(F.col("id") % 1000 == 0, F.lit(1))
            .otherwise((F.col("id") % n_accounts) + 1)
            .cast("long"),
        )
        .withColumn("TRANSACTION_TYPE_CD", _pick(TRANSACTION_TYPE_CODES, "pmod(id, 8) + 1"))
        .withColumn(
            "TRANSACTION_DATE", F.date_sub(F.lit(run_date), ((F.col("id") * 7) % 400).cast("int"))
        )
        .withColumn("TRANSACTION_TS", F.col("TRANSACTION_DATE").cast("timestamp"))
        .withColumn("AMOUNT", (((F.col("id") * 313) % 500000) / 100.0).cast("decimal(15,2)"))
        .withColumn(
            "RUNNING_BALANCE", (((F.col("id") * 761) % 900000) / 100.0).cast("decimal(15,2)")
        )
        .withColumn("MERCHANT_NAME", F.concat(F.lit("MERCHANT_"), (F.col("id") * 17) % 5000))
        .withColumn("MERCHANT_CATEGORY", _pick(MERCHANT_CATEGORIES, "pmod(id, 10) + 1"))
        .withColumn("CHANNEL_CODE", _pick(CHANNELS, "pmod(id, 6) + 1"))
        .withColumn("REFERENCE_NUM", F.concat(F.lit("REF"), F.col("id")))
        .withColumn(
            "STATUS_CODE",
            F.when(F.col("id") % 97 == 0, F.lit("R"))
            .when(F.col("id") % 89 == 0, F.lit("H"))
            .otherwise(F.lit("P")),
        )
        .withColumn("CREATED_TS", F.col("TRANSACTION_TS"))
    )
    return enforce_schema(df, schemas.TRANSACTIONS)


def generate_sources(
    spark: SparkSession, n_customers: int, *, run_date: date
) -> dict[TableSpec, DataFrame]:
    """All six source tables at the requested customer count."""

    return {
        schemas.CUSTOMERS: customers(spark, n_customers, run_date=run_date),
        schemas.ADDRESSES: addresses(spark, n_customers, run_date=run_date),
        schemas.ACCOUNTS: accounts(spark, n_customers, run_date=run_date),
        schemas.CUSTOMER_BUREAU_SCORES: bureau_scores(spark, n_customers, run_date=run_date),
        schemas.TRANSACTION_TYPES: transaction_types(spark, run_date=run_date),
        schemas.TRANSACTIONS: transactions(spark, n_customers, run_date=run_date),
    }


def load_sources(io: DataIO, frames: Mapping[TableSpec, DataFrame]) -> dict[str, int]:
    """Materialise generated sources through the IO layer the jobs read from."""

    return {
        spec.qualified_name: io.write_spec(df, spec, mode="overwrite")
        for spec, df in frames.items()
    }
