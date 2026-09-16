from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def log_step(spark, step: str, status: str, msg: str = "", rowcount: int | None = None,
             audit_table: str | None = None) -> None:
    """Mirror of sas/macros/log_step.sas: prints [PIPELINE] lines and appends
    a row to the audit Delta table when audit_table is provided."""
    print("=" * 64)
    print(f"[PIPELINE] {_ts()} | {step} | {status}")
    if msg:
        print(f"[PIPELINE] {msg}")
    if rowcount is not None:
        print(f"[PIPELINE] Rows: {rowcount}")
    print("=" * 64)

    if audit_table and spark is not None:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {audit_table} (
                job_name STRING,
                status STRING,
                message STRING,
                row_count BIGINT,
                log_ts TIMESTAMP
            ) USING DELTA
        """)
        row = [(step, status, msg, rowcount, datetime.now())]
        df = spark.createDataFrame(
            row, ["job_name", "status", "message", "row_count", "log_ts"])
        df.write.format("delta").mode("append").saveAsTable(audit_table)


@contextmanager
def timed_step(spark, step: str, audit_table: str | None = None):
    log_step(spark, step, "START", audit_table=audit_table)
    t0 = time.time()
    yield
    log_step(spark, step, "SUCCESS", msg=f"elapsed {time.time() - t0:.2f}s",
             audit_table=audit_table)
