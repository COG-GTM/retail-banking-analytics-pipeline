"""PySpark staging layer for the retail banking analytics pipeline.

This package reimplements the former Teradata BTEQ staging transforms
(``bteq/*.bteq``) as idiomatic PySpark jobs. Each module exposes a pure
``transform(...)`` function (unit-tested in ``tests/``) plus a ``run(...)``
entry point used by :mod:`staging.run_staging_pipeline`.
"""
