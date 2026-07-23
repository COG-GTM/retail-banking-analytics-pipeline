"""Shared utilities for the Databricks retail-banking analytics pipeline.

Modules:
    config      - Ticket 2: widget / job-parameter + Databricks Secrets driven config.
    audit       - Ticket 3: Delta-backed audit log (init_audit / log_step).
    validation  - Ticket 3: data-quality checks that raise on failure.
    spark_utils - Delta-enabled SparkSession helper for local runs and tests.
"""
