"""Shared helpers for the Databricks port of the retail banking analytics pipeline.

These modules replace the SAS macro library (``sas/macros``) and the shell
configuration file (``config/pipeline_config.cfg``):

===========================  ==================================================
Legacy artifact              Databricks replacement
===========================  ==================================================
``%connect_teradata``        ``shared.config`` + Unity Catalog / secret scopes
``%log_step`` / ETL_RUN_LOG  ``shared.audit``
``%validate_table``          ``shared.validation``
``config/pipeline_config``   ``shared.config.PipelineConfig`` (job parameters)
DDL type contracts           ``shared.schemas``
``PROC LOGISTIC``            ``shared.modeling``
===========================  ==================================================
"""
