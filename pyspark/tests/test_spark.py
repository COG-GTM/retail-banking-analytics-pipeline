"""Unit tests for common.spark (Delta-enabled SparkSession factory)."""
from __future__ import annotations

from common.spark import get_spark


def test_get_spark_returns_active_session(spark):
    assert get_spark() is spark


def test_delta_extensions_enabled(spark):
    extensions = spark.conf.get("spark.sql.extensions", "")
    assert "DeltaSparkSessionExtension" in extensions


def test_can_write_and_read_delta(spark, tmp_path):
    path = str(tmp_path / "delta_smoke")
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING")
    df.write.format("delta").mode("overwrite").save(path)
    assert spark.read.format("delta").load(path).count() == 2
