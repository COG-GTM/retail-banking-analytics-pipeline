"""PySpark migration of the SAS risk-scoring vertical slice.

Source of truth: ``sas/03_sas_risk_scoring.sas`` (STEP 1 - STEP 6) and its
upstream feature builder ``bteq/03_stg_risk_factors.bteq``.

Module map (see ``docs/pyspark_migration_contract.md``):

===================  ==========================================================
Module               SAS / BTEQ origin
===================  ==========================================================
``config``           ``config/pipeline_config.cfg``, ``%let`` macro variables
``session``          SAS session startup
``connections``      ``sas/macros/connect_teradata.sas``
``schemas``          ``ddl/01_staging_tables.sql``, ``ddl/02_data_product_tables.sql``
``ingestion``        STEP 1 (``WORK.RISK_RAW``), STEP 2 (``WORK.RISK_FEATURES``)
``model``            STEP 3 (``PROC LOGISTIC`` -> ``WORK.RISK_SCORED``)
``scoring``          STEP 4 (``WORK.RISK_CLASSIFIED`` / ``CUSTOMER_RISK_FINAL``)
``validation``       ``sas/macros/validate_table.sas`` (STEP 5)
``audit``            ``sas/macros/log_step.sas`` (``%init_audit`` / ``%log_step``)
``sink``             STEP 6 (``DELETE`` + ``PROC APPEND FORCE``)
``driver``           the program itself, STEP 1 -> STEP 6
===================  ==========================================================
"""

__all__ = ["config", "connections", "schemas", "session"]
