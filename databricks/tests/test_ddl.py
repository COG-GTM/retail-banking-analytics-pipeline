"""Ticket 1 - Delta DDL rendering & statement splitting."""
from __future__ import annotations

from common import ddl
from common.config import PipelineConfig


def test_render_sql_substitutes_catalog_and_schema_placeholders():
    cfg = PipelineConfig(catalog="mycat")
    rendered = ddl.render_sql(
        "CREATE TABLE ${catalog}.${staging_schema}.t (id INT)", cfg
    )
    assert rendered == "CREATE TABLE mycat.etl_staging.t (id INT)"


def test_split_statements_ignores_semicolons_inside_comments():
    sql = """
    -- a comment; with an embedded semicolon
    CREATE TABLE a (id INT);
    -- another; comment
    CREATE TABLE b (id INT);
    """
    stmts = ddl.split_statements(sql)
    assert len(stmts) == 2
    assert stmts[0].startswith("CREATE TABLE a")
    assert stmts[1].startswith("CREATE TABLE b")


def test_delta_ddl_files_have_no_teradata_only_syntax():
    banned = ["MULTISET", "PRIMARY INDEX", "COLLECT STATISTICS", "CHARACTER SET"]
    for path in sorted(ddl.DDL_DIR.glob("*.sql")):
        # Strip comments: the header comments *describe* the removed Teradata
        # clauses, so only executable SQL is checked.
        text = ddl._strip_comments(path.read_text()).upper()
        for token in banned:
            assert token not in text, f"{path.name} still contains {token}"


def test_transaction_analytics_is_partitioned_by_reporting_period():
    text = (ddl.DDL_DIR / "03_data_product_tables.sql").read_text().lower()
    assert "partitioned by (reporting_period)" in text


def test_create_all_builds_catalog_schemas_and_tables(spark, config):
    ddl.create_all(spark, config)
    schemas = {r.namespace for r in spark.sql("SHOW SCHEMAS IN spark_catalog").collect()}
    assert {"core_banking", "txn_processing", "etl_staging", "data_products"} <= schemas
    tables = {
        r.tableName for r in spark.sql("SHOW TABLES IN spark_catalog.data_products").collect()
    }
    assert {
        "customer_segments", "transaction_analytics",
        "customer_risk_scores", "customer_master_profile",
    } <= tables
