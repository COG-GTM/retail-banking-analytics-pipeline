from pathlib import Path

from retail_banking.bronze.ingest import ingest_batch
from retail_banking.config import RunConfig
from retail_banking.gold import (
    customer_master_profile,
    customer_risk_scores,
    customer_segments,
    transaction_analytics,
)
from retail_banking.silver import stg_customer_360, stg_risk_factors, stg_txn_summary

REPO_ROOT = Path(__file__).parents[2]


def run_bronze(spark, cfg: RunConfig):
    ingest_batch(spark, cfg, cfg.source_path or str(REPO_ROOT / "data" / "01_source_tables"))


def run_silver(spark, cfg: RunConfig):
    stg_customer_360.run(spark, cfg)
    stg_txn_summary.run(spark, cfg)
    stg_risk_factors.run(spark, cfg)


def run_gold(spark, cfg: RunConfig):
    customer_segments.run(spark, cfg)
    transaction_analytics.run(spark, cfg)
    customer_risk_scores.run(spark, cfg)
    return customer_master_profile.run(spark, cfg)


def main():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.jars.repositories", "https://maven.aliyun.com/repository/central")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    cfg = RunConfig(
        catalog=None,
        run_date=__import__("datetime").date(2026, 4, 10),
        dq_min_rows=1,
        mlflow_enabled=False,
    )
    run_bronze(spark, cfg)
    run_silver(spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
