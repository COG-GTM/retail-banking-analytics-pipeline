"""Synthetic source-data generator + in-memory IO for e2e / performance tiers.

``synthetic_sources`` builds fully-typed source DataFrames at an arbitrary scale
using ``spark.range`` + deterministic column expressions (no Python row loops, so
it scales to the 10M-customer performance target). ``DictDataIO`` is a
disk-free :class:`~common.io.DataIO` backend that serves those sources and holds
staging/product outputs in memory so the whole DAG can be chained end-to-end.
"""

from __future__ import annotations

import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import schemas
from common.config import PipelineConfig
from common.io import DataIO

# Domains mirror the committed source fixtures so every downstream filter
# (status_code='P', category DEBIT/CREDIT/FEE, channel mix, account_status='O')
# actually matches rows.
_CHANNELS = ["ACH", "POS", "WEB", "ATM", "MOB"]
_CATEGORIES = ["GROCERY", "GAS_STATION", "RESTAURANT", "RETAIL", "TRAVEL", "UTILITIES"]
_ACCT_TYPES = ["CHECKING", "SAVINGS", "CREDIT", "LOAN"]
# (code, description, category, is_revenue)
_TXN_TYPES = [
    ("PUR", "Purchase", "DEBIT", "N"),
    ("WDR", "Withdrawal", "DEBIT", "N"),
    ("TRF", "Transfer", "DEBIT", "N"),
    ("DEP", "Deposit", "CREDIT", "N"),
    ("PMT", "Payment", "CREDIT", "N"),
    ("INT", "Interest", "CREDIT", "Y"),
    ("FEE", "Account Fee", "FEE", "Y"),
    ("NSF", "NSF Fee", "FEE", "Y"),
]
_DEBIT_CODES = ["PUR", "WDR", "TRF"]
_CREDIT_CODES = ["DEP", "PMT", "INT"]
_FEE_CODES = ["FEE", "NSF"]


def synthetic_sources(
    spark: SparkSession,
    config: PipelineConfig,
    n_customers: int = 1000,
    accounts_per_customer: int = 2,
    txns_per_account: int = 30,
    seed: int = 42,
) -> dict[str, DataFrame]:
    """Return a dict of source-table DataFrames scaled to ``n_customers``."""
    run_date = config.run_date

    # ---- customers ----------------------------------------------------------
    cust = (
        spark.range(1, n_customers + 1).withColumnRenamed("id", "customer_id")
        .withColumn("first_name", F.concat(F.lit("Cust"), F.col("customer_id").cast("string")))
        .withColumn("last_name", F.concat(F.lit("Fam"), (F.col("customer_id") % 500).cast("string")))
        .withColumn("date_of_birth", F.expr(f"date_sub(date'{run_date}', cast(6570 + pmod(customer_id*37, 21900) as int))"))
        .withColumn("ssn_hash", F.sha2(F.col("customer_id").cast("string"), 256))
        .withColumn("email", F.concat(F.lit("c"), F.col("customer_id").cast("string"), F.lit("@example.com")))
        .withColumn("phone_primary", F.concat(F.lit("555"), F.lpad((F.col("customer_id") % 10000).cast("string"), 4, "0")))
        .withColumn("customer_since", F.expr(f"date_sub(date'{run_date}', cast(30 + pmod(customer_id*53, 5000) as int))"))
        .withColumn("customer_status", F.when(F.col("customer_id") % 20 == 0, F.lit("I")).otherwise(F.lit("A")))
        .withColumn("segment_code", F.element_at(F.array(*[F.lit(x) for x in ("MASS", "PREMIER", "DIGITAL", "WEALTH")]), (F.col("customer_id") % 4 + 1).cast("int")))
        .withColumn("branch_id", (F.col("customer_id") % 300 + 1).cast("int"))
        .withColumn("created_ts", F.lit(_dt.datetime(2020, 1, 1)))
        .withColumn("updated_ts", F.lit(_dt.datetime(2020, 1, 1)))
    )

    # ---- accounts (accounts_per_customer per customer) ----------------------
    acct_offsets = F.explode(F.array(*[F.lit(i) for i in range(accounts_per_customer)]))
    accounts = (
        cust.select("customer_id").withColumn("k", acct_offsets)
        .withColumn("account_id", F.col("customer_id") * 10 + F.col("k"))
        .withColumn("account_type", F.element_at(F.array(*[F.lit(x) for x in _ACCT_TYPES]), (F.col("k") % len(_ACCT_TYPES) + 1).cast("int")))
        .withColumn("account_status", F.when(F.col("account_id") % 25 == 0, F.lit("C")).when(F.col("account_id") % 40 == 0, F.lit("F")).otherwise(F.lit("O")))
        .withColumn("open_date", F.expr(f"date_sub(date'{run_date}', cast(60 + pmod(account_id*29, 4000) as int))"))
        .withColumn("close_date", F.when(F.col("account_status") == "C", F.expr(f"date_sub(date'{run_date}', 10)")).otherwise(F.lit(None).cast("date")))
        .withColumn("current_balance", (F.pmod(F.col("account_id") * 17, F.lit(50000)) + 100).cast("decimal(15,2)"))
        .withColumn("available_balance", F.col("current_balance"))
        .withColumn("credit_limit", F.when(F.col("account_type") == "CREDIT", F.lit(10000).cast("decimal(15,2)")).otherwise(F.lit(0).cast("decimal(15,2)")))
        .withColumn("interest_rate", F.lit(0.0125).cast("decimal(5,4)"))
        .withColumn("branch_id", (F.col("customer_id") % 300 + 1).cast("int"))
        .withColumn("created_ts", F.lit(_dt.datetime(2020, 1, 1)))
        .withColumn("updated_ts", F.lit(_dt.datetime(2020, 1, 1)))
        .drop("k")
    )

    # ---- addresses (one primary per customer) -------------------------------
    addresses = (
        cust.select("customer_id")
        .withColumn("address_id", F.col("customer_id"))
        .withColumn("address_type", F.lit("HOME"))
        .withColumn("address_line_1", F.concat((F.col("customer_id") % 9999).cast("string"), F.lit(" Main St")))
        .withColumn("address_line_2", F.lit(None).cast("string"))
        .withColumn("city", F.concat(F.lit("City"), (F.col("customer_id") % 200).cast("string")))
        .withColumn("state_code", F.element_at(F.array(*[F.lit(x) for x in ("CA", "TX", "NY", "FL", "NC", "WA")]), (F.col("customer_id") % 6 + 1).cast("int")))
        .withColumn("zip_code", F.lpad((F.col("customer_id") % 99999).cast("string"), 5, "0"))
        .withColumn("country_code", F.lit("US"))
        .withColumn("is_primary", F.lit("Y"))
        .withColumn("effective_date", F.expr(f"date_sub(date'{run_date}', 400)"))
        .withColumn("expiration_date", F.lit(None).cast("date"))
        .withColumn("created_ts", F.lit(_dt.datetime(2020, 1, 1)))
        .withColumn("updated_ts", F.lit(_dt.datetime(2020, 1, 1)))
    )

    # ---- transaction_types (small broadcastable dimension) ------------------
    txn_types = spark.createDataFrame(
        [(cd, desc, cat, rev, _dt.date(2019, 1, 1), None) for cd, desc, cat, rev in _TXN_TYPES],
        schema=schemas.TRANSACTION_TYPES.struct,
    )

    # ---- transactions (txns_per_account per account, within lookback) -------
    txn_offsets = F.explode(F.array(*[F.lit(i) for i in range(txns_per_account)]))
    lookback_days = (run_date - config.lookback_start).days
    txns = (
        accounts.select("account_id").withColumn("t", txn_offsets)
        .withColumn("transaction_id", F.col("account_id") * 1000 + F.col("t"))
        .withColumn("transaction_type_cd", F.element_at(F.array(*[F.lit(x[0]) for x in _TXN_TYPES]), (F.col("t") % len(_TXN_TYPES) + 1).cast("int")))
        .withColumn("transaction_date", F.expr(f"date_sub(date'{run_date}', cast(pmod(transaction_id*13, {max(lookback_days,1)}) as int))"))
        .withColumn("transaction_ts", F.col("transaction_date").cast("timestamp"))
        .withColumn(
            "amount",
            F.when(F.col("transaction_type_cd").isin(_CREDIT_CODES), (F.pmod(F.col("transaction_id") * 7, F.lit(2000)) + 50).cast("decimal(15,2)"))
            .when(F.col("transaction_type_cd").isin(_FEE_CODES), F.lit(-35).cast("decimal(15,2)"))
            .otherwise((F.pmod(F.col("transaction_id") * 11, F.lit(1500)) + 5).cast("decimal(15,2)") * F.lit(-1)),
        )
        .withColumn("running_balance", F.lit(1000).cast("decimal(15,2)"))
        .withColumn("merchant_name", F.concat(F.lit("MERCH_"), (F.col("transaction_id") % 400).cast("string")))
        .withColumn("merchant_category", F.element_at(F.array(*[F.lit(x) for x in _CATEGORIES]), (F.col("transaction_id") % len(_CATEGORIES) + 1).cast("int")))
        .withColumn("channel_code", F.element_at(F.array(*[F.lit(x) for x in _CHANNELS]), (F.col("transaction_id") % len(_CHANNELS) + 1).cast("int")))
        .withColumn("reference_num", F.col("transaction_id").cast("string"))
        # Mostly posted ('P') with some held/reversed, mirroring the source mix.
        .withColumn("status_code", F.when(F.col("transaction_id") % 5 == 4, F.lit("H")).otherwise(F.lit("P")))
        .withColumn("created_ts", F.lit(_dt.datetime(2020, 1, 1)))
        .drop("t")
    )

    # ---- bureau scores ------------------------------------------------------
    bureau = (
        cust.select("customer_id")
        .withColumn("external_credit_score", (F.pmod(F.col("customer_id") * 41, F.lit(500)) + 350).cast("int"))
        .withColumn("report_date", F.expr(f"date_sub(date'{run_date}', 15)"))
    )

    return {
        "CUSTOMERS": schemas.enforce_schema(cust, schemas.CUSTOMERS),
        "ACCOUNTS": schemas.enforce_schema(accounts, schemas.ACCOUNTS),
        "ADDRESSES": schemas.enforce_schema(addresses, schemas.ADDRESSES),
        "TRANSACTIONS": schemas.enforce_schema(txns, schemas.TRANSACTIONS),
        "TRANSACTION_TYPES": schemas.enforce_schema(txn_types, schemas.TRANSACTION_TYPES),
        "CUSTOMER_BUREAU_SCORES": schemas.enforce_schema(bureau, schemas.CUSTOMER_BUREAU_SCORES),
    }


class DictDataIO(DataIO):
    """In-memory IO backend: reads generated sources, holds outputs in dicts."""

    def __init__(self, spark, config, sources: dict[str, DataFrame]):
        super().__init__(spark, config)
        self._sources = sources
        self._staging: dict[str, DataFrame] = {}
        self._products: dict[str, DataFrame] = {}

    def read_source(self, table: str) -> DataFrame:
        return self._sources[table]

    def read_staging(self, table: str) -> DataFrame:
        return self._staging[table]

    def read_data_product(self, table: str) -> DataFrame:
        return self._products[table]

    def write_staging(self, df: DataFrame, table: str) -> None:
        self._staging[table] = df.cache()
        self._staging[table].count()  # materialise so downstream reads are cheap

    def write_data_product(self, df: DataFrame, table: str) -> None:
        self._products[table] = df.cache()
        self._products[table].count()
