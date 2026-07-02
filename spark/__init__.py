"""PySpark migration of the SAS analytics layer.

This package replaces the SAS programs (``sas/01``-``sas/04``) and the
``connect_teradata`` SAS/ACCESS macro with idiomatic PySpark + Spark ML jobs.

The Teradata ``LIBNAME`` connections established by ``connect_teradata`` are
replaced by :class:`spark.session.DataLayer`, which reads the same BTEQ staging
datasets and writes the same certified data-product datasets, preserving the
column/semantic contract consumed downstream.
"""
