"""DDL bootstrap: create the Spark/Delta databases and tables.

Reads the parameterized Spark SQL DDL in ``ddl/*.sql``, resolves ``${...}``
placeholders from the environment (populated by ``config/pipeline_config.cfg``),
and executes each statement against the configured Spark catalog.

Replaces the Teradata DDL that was previously applied out-of-band. The source
tables (``ddl/00_source_tables.sql``) are documentation only and are NOT
created here -- only the staging and data-product schemas are provisioned.

Usage (via spark-submit):
    spark-submit --py-files spark/spark_session.py spark/apply_ddl.py
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from string import Template

from spark_session import build_spark

LOG = logging.getLogger("apply_ddl")

# Source-table DDL is documentation only; only staging + data products are
# provisioned by the bootstrap.
DDL_FILES = (
    "01_staging_tables.sql",
    "02_data_product_tables.sql",
)


def render(sql_text: str) -> str:
    """Substitute ${VAR} placeholders from the environment."""
    return Template(sql_text).safe_substitute(os.environ)


def iter_statements(sql_text: str):
    """Yield individual SQL statements from a script.

    Line comments (``--`` to end of line) are stripped first so that any
    semicolons appearing inside comments do not split statements.
    """
    no_comments = "\n".join(line.split("--", 1)[0] for line in sql_text.splitlines())
    for raw in no_comments.split(";"):
        stmt = raw.strip()
        if stmt:
            yield stmt


def ddl_dir() -> Path:
    configured = os.environ.get("DDL_DIR")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parent.parent / "ddl"


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] [apply_ddl] %(message)s",
    )

    base = ddl_dir()
    spark = build_spark("ddl")
    try:
        for fname in DDL_FILES:
            path = base / fname
            if not path.exists():
                LOG.error("DDL file not found: %s", path)
                return 1
            LOG.info("Applying DDL file: %s", path)
            for stmt in iter_statements(render(path.read_text())):
                LOG.info("Executing: %s", stmt.splitlines()[0].strip())
                spark.sql(stmt)
        LOG.info("All DDL applied successfully.")
    except Exception:  # noqa: BLE001 - log full context then fail non-zero
        LOG.exception("DDL bootstrap failed.")
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
