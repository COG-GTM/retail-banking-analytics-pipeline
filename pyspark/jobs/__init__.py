"""ETL job modules, one per legacy BTEQ/SAS program.

Each module exposes pure ``transform``-style functions (unit-testable on
in-memory DataFrames) plus a ``run(spark, io, config, audit)`` entry point that
reads sources, validates, writes to the target and records the audit trail.
"""
