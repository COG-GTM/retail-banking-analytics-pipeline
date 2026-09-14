from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def _schema_name(fqn: str) -> str:
    parts = fqn.split(".")
    return ".".join(parts[:-1])


def ensure_schema(spark: SparkSession, fqn: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema_name(fqn)}")


def write_overwrite(df: DataFrame, fqn: str) -> None:
    ensure_schema(df.sparkSession, fqn)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)


def write_overwrite_partition(df: DataFrame, fqn: str, partition_col: str, value) -> None:
    ensure_schema(df.sparkSession, fqn)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_col} = '{value}'")
        .option("overwriteSchema", "true")
        .partitionBy(partition_col)
        .saveAsTable(fqn)
    )


def merge_upsert(spark: SparkSession, df: DataFrame, fqn: str, key_cols: list[str]) -> None:
    ensure_schema(spark, fqn)
    if not spark.catalog.tableExists(fqn):
        df.write.format("delta").mode("overwrite").saveAsTable(fqn)
        return
    condition = " AND ".join(f"target.{c} = source.{c}" for c in key_cols)
    (
        DeltaTable.forName(spark, fqn)
        .alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append(df: DataFrame, fqn: str) -> None:
    ensure_schema(df.sparkSession, fqn)
    df.write.format("delta").mode("append").saveAsTable(fqn)
