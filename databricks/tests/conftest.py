import sys
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    derby = tmp_path_factory.mktemp("derby")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retail-banking-local-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={derby}/metastore_db;create=true",
        )
        .config("spark.jars.repositories", "https://maven.aliyun.com/repository/central")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()
