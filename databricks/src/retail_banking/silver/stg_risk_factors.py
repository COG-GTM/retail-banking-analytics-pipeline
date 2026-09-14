from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..audit import assert_rows, step
from ..config import RunConfig
from ..dq import validate_table
from ..tables import write_overwrite


def _daily_balance(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    txns = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transactions"))
    accounts = spark.table(cfg.fqn(cfg.bronze_core_schema, "accounts")).select(
        "account_id", "customer_id"
    )
    latest = (
        txns.join(accounts, "account_id")
        .where(
            (F.col("transaction_date") >= F.add_months(F.lit(cfg.run_date), -3))
            & (F.col("status_code") == "P")
        )
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("account_id", "transaction_date").orderBy(
                    F.col("transaction_ts").desc()
                )
            ),
        )
        .where(F.col("_rn") == 1)
    )
    return latest.select("customer_id", "account_id", "transaction_date", "running_balance")


def _payment_history(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    txns = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transactions"))
    types = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transaction_types")).select(
        "transaction_type_cd", "category"
    )
    accounts = spark.table(cfg.fqn(cfg.bronze_core_schema, "accounts")).select(
        "account_id", "customer_id", "account_type", "open_date"
    )
    due_date = F.add_months(
        F.col("open_date"),
        F.months_between(F.col("transaction_date"), F.col("open_date")).cast("int") + 1,
    )
    return (
        txns.join(accounts, "account_id")
        .join(types, "transaction_type_cd")
        .where(
            F.col("account_type").isin("CREDIT", "LOAN")
            & (F.col("category") == "CREDIT")
            & (F.col("status_code") == "P")
            & (F.col("transaction_date") >= F.add_months(F.lit(cfg.run_date).cast("date"), -24))
        )
        .withColumn("_late", F.col("transaction_date") > due_date)
        .groupBy("customer_id", "account_id")
        .agg(
            F.count("*").alias("total_payments"),
            F.sum(F.when(~F.col("_late"), 1).otherwise(0)).alias("ontime_payments"),
            F.sum(F.when(F.col("_late"), 1).otherwise(0)).alias("late_payments"),
            F.coalesce(
                F.max(F.when(F.col("_late"), F.col("transaction_date"))), F.min("open_date")
            ).alias("_last_late_or_open"),
        )
        .withColumn(
            "months_since_last_late",
            (
                (F.year(F.lit(cfg.run_date)) - F.year("_last_late_or_open")) * 12
                + F.month(F.lit(cfg.run_date))
                - F.month("_last_late_or_open")
            ).cast("int"),
        )
        .drop("_last_late_or_open")
    )


def _merchant_metrics(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    txns = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transactions"))
    accounts = spark.table(cfg.fqn(cfg.bronze_core_schema, "accounts")).select(
        "account_id", "customer_id"
    )
    cutoff = F.date_sub(F.lit(cfg.run_date).cast("date"), 30)
    recent = (
        txns.where(
            (F.col("transaction_date") >= cutoff)
            & F.col("merchant_name").isNotNull()
            & (F.col("status_code") == "P")
        )
        .select("account_id", "merchant_name")
        .distinct()
    )
    new_merchants = (
        recent.join(accounts, "account_id")
        .groupBy("customer_id")
        .agg(F.countDistinct("merchant_name").alias("new_merchant_cnt_30d"))
    )
    six_month = (
        txns.join(accounts, "account_id")
        .where(
            (F.col("transaction_date") >= F.add_months(F.lit(cfg.run_date), -6))
            & (F.col("status_code") == "P")
        )
        .groupBy("customer_id")
        .agg(
            F.sum(F.when(F.col("channel_code") == "INTL", 1).otherwise(0)).alias(
                "international_txn_cnt"
            ),
            F.sum(
                F.when(
                    F.col("merchant_category").isin(
                        "GAMBLING", "WIRE_TRANSFER_INTL", "CRYPTO_EXCHANGE", "PAWN_SHOP"
                    ),
                    1,
                ).otherwise(0)
            ).alias("high_risk_merchant_cnt"),
        )
    )
    return new_merchants.join(six_month, "customer_id", "full")


def build(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    customers = spark.table(cfg.fqn(cfg.bronze_core_schema, "customers"))
    accounts = spark.table(cfg.fqn(cfg.bronze_core_schema, "accounts"))
    txns = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transactions"))
    types = spark.table(cfg.fqn(cfg.bronze_txn_schema, "transaction_types")).select(
        "transaction_type_cd", "category", "description"
    )
    bureau = spark.table(cfg.fqn(cfg.bronze_core_schema, "customer_bureau_scores"))
    run_date = F.lit(cfg.run_date).cast("date")

    daily = _daily_balance(spark, cfg)
    balance = daily.groupBy("customer_id").agg(
        F.avg(
            F.when(F.col("transaction_date") >= F.date_sub(run_date, 30), F.col("running_balance"))
        ).alias("avg_daily_balance_30d"),
        F.avg(
            F.when(F.col("transaction_date") >= F.date_sub(run_date, 90), F.col("running_balance"))
        ).alias("avg_daily_balance_90d"),
        F.stddev_pop("running_balance").alias("balance_volatility"),
    )
    payments = (
        _payment_history(spark, cfg)
        .groupBy("customer_id")
        .agg(
            F.sum("total_payments").alias("total_payments"),
            F.sum("ontime_payments").alias("ontime_payments"),
            F.sum("late_payments").alias("payment_late_cnt"),
            F.min("months_since_last_late").alias("months_since_last_late"),
        )
    )
    credit = (
        accounts.where((F.col("account_type") == "CREDIT") & (F.col("account_status") == "O"))
        .groupBy("customer_id")
        .agg(
            F.sum(F.coalesce("current_balance", F.lit(0))).alias("total_credit_bal"),
            F.sum(F.coalesce("credit_limit", F.lit(0))).alias("total_credit_limit"),
        )
    )
    posted = (
        txns.join(accounts.select("account_id", "customer_id"), "account_id")
        .join(types, "transaction_type_cd")
        .where(
            (F.col("transaction_date") >= F.add_months(run_date, -12))
            & (F.col("status_code") == "P")
        )
    )
    overdraft = posted.groupBy("customer_id").agg(
        F.sum(F.when(F.col("running_balance") < 0, 1).otherwise(0)).alias("account_overdraft_cnt"),
        F.sum(
            F.when(
                (F.col("category") == "FEE") & F.col("description").like("%NSF%"),
                F.abs("amount"),
            ).otherwise(0)
        ).alias("nsf_fee_total"),
    )
    large_withdrawal = (
        posted.where((F.col("category") == "DEBIT") & (F.abs("amount") >= 5000))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("large_withdrawal_cnt"),
            F.sum(F.abs("amount")).alias("large_withdrawal_amt"),
        )
    )
    velocity = (
        posted.where(
            (F.col("category") == "DEBIT") & (F.col("transaction_date") >= F.date_sub(run_date, 30))
        )
        .groupBy("customer_id")
        .agg(
            F.sum(
                F.when(
                    F.col("transaction_date") >= F.date_sub(run_date, 7), F.abs("amount")
                ).otherwise(0)
            ).alias("debit_velocity_7d"),
            F.sum(F.abs("amount")).alias("debit_velocity_30d"),
        )
    )
    bureau_window = Window.partitionBy("customer_id").orderBy(F.col("report_date").desc())
    latest_bureau = (
        bureau.withColumn("_rn", F.row_number().over(bureau_window))
        .where(F.col("_rn") == 1)
        .select("customer_id", F.col("external_credit_score"))
    )
    merchant = _merchant_metrics(spark, cfg)

    joined = (
        customers.where(F.col("customer_status").isin("A", "I"))
        .join(overdraft, "customer_id", "left")
        .join(large_withdrawal, "customer_id", "left")
        .join(balance, "customer_id", "left")
        .join(credit, "customer_id", "left")
        .join(payments, "customer_id", "left")
        .join(latest_bureau, "customer_id", "left")
        .join(velocity, "customer_id", "left")
        .join(merchant, "customer_id", "left")
    )
    return joined.select(
        "customer_id",
        F.coalesce("account_overdraft_cnt", F.lit(0)).cast("int").alias("account_overdraft_cnt"),
        F.coalesce("nsf_fee_total", F.lit(0)).cast("decimal(15,2)").alias("nsf_fee_total"),
        F.coalesce("large_withdrawal_cnt", F.lit(0)).cast("int").alias("large_withdrawal_cnt"),
        F.coalesce("large_withdrawal_amt", F.lit(0))
        .cast("decimal(18,2)")
        .alias("large_withdrawal_amt"),
        F.coalesce("avg_daily_balance_30d", F.lit(0))
        .cast("decimal(15,2)")
        .alias("avg_daily_balance_30d"),
        F.coalesce("avg_daily_balance_90d", F.lit(0))
        .cast("decimal(15,2)")
        .alias("avg_daily_balance_90d"),
        F.coalesce("balance_volatility", F.lit(0))
        .cast("decimal(10,4)")
        .alias("balance_volatility"),
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("total_credit_bal") / F.col("total_credit_limit")).cast("decimal(5,4)"),
        )
        .otherwise(F.lit(0).cast("decimal(5,4)"))
        .alias("credit_util_ratio"),
        F.when(
            F.col("total_payments") > 0,
            (F.col("ontime_payments") * 100.0 / F.col("total_payments")).cast("decimal(5,2)"),
        )
        .otherwise(F.lit(100).cast("decimal(5,2)"))
        .alias("payment_ontime_pct"),
        F.coalesce("payment_late_cnt", F.lit(0)).cast("int").alias("payment_late_cnt"),
        F.coalesce("months_since_last_late", F.lit(999))
        .cast("int")
        .alias("months_since_last_late"),
        F.coalesce("external_credit_score", F.lit(0)).cast("int").alias("external_credit_score"),
        F.coalesce("debit_velocity_7d", F.lit(0)).cast("decimal(15,2)").alias("debit_velocity_7d"),
        F.coalesce("debit_velocity_30d", F.lit(0))
        .cast("decimal(15,2)")
        .alias("debit_velocity_30d"),
        F.coalesce("new_merchant_cnt_30d", F.lit(0)).cast("int").alias("new_merchant_cnt_30d"),
        F.coalesce("international_txn_cnt", F.lit(0)).cast("int").alias("international_txn_cnt"),
        F.coalesce("high_risk_merchant_cnt", F.lit(0)).cast("int").alias("high_risk_merchant_cnt"),
        F.current_timestamp().alias("load_ts"),
    )


def run(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    fqn = cfg.fqn(cfg.silver_schema, "stg_risk_factors")
    with step(spark, cfg, "03_stg_risk_factors", "FULL_LOAD") as state:
        result = build(spark, cfg)
        state["row_count"] = assert_rows(result, "stg_risk_factors")
        write_overwrite(result, fqn)
    validate_table(spark, fqn, ["customer_id"], ["customer_id"], cfg.dq_min_rows)
    return result
