"""Shared helpers for the Databricks retail banking analytics pipeline.

Replaces the SAS macro library (``sas/macros/``):

===========================  ==========================================
SAS macro                    Databricks replacement
===========================  ==========================================
``%init_audit`` /             :mod:`shared.audit` (Delta audit table)
``%log_step``
``%validate_table``           :mod:`shared.dq`
``%connect_teradata``         Unity Catalog (no credentials required)
===========================  ==========================================
"""
