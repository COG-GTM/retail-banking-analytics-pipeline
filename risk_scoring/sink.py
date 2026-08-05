"""STEP 6 — load ``DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES``.

The SAS being replaced::

    proc sql;
        connect to teradata (...);
        execute (DELETE FROM DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES) by teradata;
        disconnect from teradata;
    quit;

    proc append base=DPDB.CUSTOMER_RISK_SCORES data=WORK.CUSTOMER_RISK_FINAL force;
    run;

The target has no partition column, so ``DELETE`` followed by ``PROC APPEND``
is a full truncate-load and maps cleanly onto ``mode("overwrite")`` (with
``truncate=true`` on the JDBC path, so the table definition survives exactly as
``DELETE FROM`` left it). Collapsing the two statements into one write also
makes the step idempotent, which the SAS was not: a failure between the
``DELETE`` and the ``PROC APPEND`` left the target empty.

``COLLECT STATISTICS`` (run by the surrounding Teradata jobs after a load) has
no PySpark equivalent and is dropped; optimiser statistics are a warehouse-side
concern for whoever owns the physical table.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import schemas
from .audit import AuditLog
from .connections import Connections

logger = logging.getLogger(__name__)


class SinkContractError(RuntimeError):
    """The DataFrame does not satisfy the ``CUSTOMER_RISK_SCORES`` column contract."""


def cast_to_target_schema(df: DataFrame) -> DataFrame:
    """Project to ``CUSTOMER_RISK_SCORES_COLUMNS`` and cast to the DECIMAL target.

    Intermediate DataFrames stay in ``double``; the DECIMAL types of
    ``ddl/02_data_product_tables.sql`` are applied here, once.
    """
    missing = [c for c in schemas.CUSTOMER_RISK_SCORES_COLUMNS if c not in df.columns]
    if missing:
        raise SinkContractError(
            f"{schemas.CUSTOMER_RISK_SCORES} is missing required column(s) "
            f"{', '.join(missing)}; got {', '.join(df.columns)}"
        )
    return df.select(*[
        F.col(f.name).cast(f.dataType).alias(f.name)
        for f in schemas.CUSTOMER_RISK_SCORES_SCHEMA.fields
    ])


def write_customer_risk_scores(
    df: DataFrame, connections: Connections, *, audit: AuditLog | None = None
) -> int:
    """Truncate-load ``CUSTOMER_RISK_SCORES`` and return the row count written."""
    target = schemas.CUSTOMER_RISK_SCORES
    if audit is not None:
        audit.log_step(
            step="03_RISK_SCORING",
            status="START",
            msg=f"Loading {connections.config.databases.data_products}.{target}",
        )

    out = cast_to_target_schema(df).cache()
    try:
        row_count = out.count()  # single action; the write reuses the cache
        connections.write_data_product(out, target)
    finally:
        out.unpersist()

    logger.info("sink table=%s rows=%s", target, row_count)
    if audit is not None:
        audit.log_step(
            step="03_RISK_SCORING",
            status="SUCCESS",
            msg="Pipeline complete",
            rowcount=row_count,
        )
    return row_count
