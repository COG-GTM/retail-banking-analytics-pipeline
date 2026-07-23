"""Shared pytest fixtures.

Provides a Delta-enabled local ``SparkSession`` and a ``Config`` pointed at the
built-in ``spark_catalog`` so the three-level ``catalog.schema.table`` naming in
the contract resolves locally (Unity Catalog is not available off-Databricks).
Each test gets unique, isolated schema names that are dropped on teardown, so
runs are idempotent even against a persistent local warehouse.
"""
from __future__ import annotations

import glob
import os
import shutil
import subprocess
import tempfile
import uuid
from dataclasses import replace


def _ensure_compatible_java() -> None:
    """Point JAVA_HOME at a PySpark-compatible JDK (17, then 11) if needed.

    PySpark 3.5 supports Java 8/11/17 but not newer LTS releases. CI/local shells
    may default JAVA_HOME to an incompatible JDK, so select a supported one before
    the JVM starts. No-op when the current JAVA_HOME already works.
    """
    current = os.environ.get("JAVA_HOME")
    if current and _java_major(os.path.join(current, "bin", "java")) in (8, 11, 17):
        return
    for pattern in ("*java-17*", "*java-11*", "*jdk-17*", "*jdk-11*"):
        for candidate in sorted(glob.glob(os.path.join("/usr/lib/jvm", pattern))):
            if os.path.exists(os.path.join(candidate, "bin", "java")):
                os.environ["JAVA_HOME"] = candidate
                return


def _java_major(java_bin: str) -> int | None:
    try:
        out = subprocess.run(
            [java_bin, "-version"], capture_output=True, text=True, check=False
        ).stderr
    except OSError:
        return None
    for token in out.replace('"', " ").split():
        if token[0:1].isdigit():
            major = token.split(".")[0]
            if major == "1":  # e.g. 1.8.0 -> 8
                major = token.split(".")[1]
            return int(major) if major.isdigit() else None
    return None


_ensure_compatible_java()

import pytest

from common.config import Config
from common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="rb_spark_warehouse_")
    session = get_spark("retail_banking_tests")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture()
def cfg(spark):
    """Config on the local session catalog with per-test isolated schemas."""
    suffix = uuid.uuid4().hex[:8]
    base = Config()
    conf = replace(
        base,
        catalog="spark_catalog",
        schema_core=f"{base.schema_core}_{suffix}",
        schema_txn=f"{base.schema_txn}_{suffix}",
        schema_stg=f"{base.schema_stg}_{suffix}",
        schema_dp=f"{base.schema_dp}_{suffix}",
    )
    yield conf
    for schema in (conf.schema_core, conf.schema_txn, conf.schema_stg, conf.schema_dp):
        spark.sql(f"DROP SCHEMA IF EXISTS {conf.catalog}.{schema} CASCADE")
