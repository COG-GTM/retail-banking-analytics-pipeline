from __future__ import annotations

import pyspark.sql.functions as F

CORE_TABLES = ["customers", "accounts", "addresses", "customer_bureau_scores"]
TXN_TABLES = ["transactions", "transaction_types"]
TD_TABLE_MAP = {
    "customers": "CORE_BANKING_DB.CUSTOMERS",
    "accounts": "CORE_BANKING_DB.ACCOUNTS",
    "addresses": "CORE_BANKING_DB.ADDRESSES",
    "customer_bureau_scores": "CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES",
    "transactions": "TXN_PROCESSING_DB.TRANSACTIONS",
    "transaction_types": "TXN_PROCESSING_DB.TRANSACTION_TYPES",
}
SECRET_SCOPE = "retail-banking-teradata"


def _target(cfg, name: str) -> str:
    if name in TXN_TABLES:
        return cfg.txn_table(name)
    return cfg.bronze_table(name)


def _read_jdbc(spark, dbutils) -> dict:
    host = dbutils.secrets.get(SECRET_SCOPE, "td-host")
    user = dbutils.secrets.get(SECRET_SCOPE, "td-user")
    password = dbutils.secrets.get(SECRET_SCOPE, "td-password")
    url = f"jdbc:teradata://{host}/DATABASE=CORE_BANKING_DB"
    props = {"user": user, "password": password, "driver": "com.teradata.jdbc.TeraDriver"}
    out = {}
    for name, td_table in TD_TABLE_MAP.items():
        out[name] = (spark.read.format("jdbc")
                     .option("url", url)
                     .option("dbtable", td_table)
                     .options(**props)
                     .load())
    return out


def _read_csv(spark, volume_path: str) -> dict:
    out = {}
    for name in CORE_TABLES + TXN_TABLES:
        out[name] = (spark.read.option("header", "true")
                     .option("inferSchema", "true")
                     .csv(f"{volume_path}/{name}.csv"))
    return out


def ingest_source_tables(spark, cfg, source: str = "csv",
                         volume_path: str | None = None,
                         dbutils=None) -> dict:
    """Load the 6 source tables and write them as Delta bronze tables.

    source="jdbc": Teradata over JDBC with credentials from the
    `retail-banking-teradata` secret scope. Alternative (no code needed):
    define a Unity Catalog Lakehouse Federation CONNECTION/foreign catalog
    (see ddl/00_catalog_and_schemas.sql) and read the foreign catalog
    directly as bronze.
    source="csv": demo mode, CSV files in a UC Volume.
    """
    if source == "jdbc":
        if dbutils is None:
            raise ValueError("dbutils required for jdbc source (secrets)")
        frames = _read_jdbc(spark, dbutils)
    elif source == "csv":
        frames = _read_csv(spark, volume_path or cfg.landing_volume)
    else:
        raise ValueError(f"unknown source: {source}")

    for name, df in frames.items():
        target = _target(cfg, name)
        (df.withColumn("load_ts", F.current_timestamp())
           .write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(target))
    return frames
