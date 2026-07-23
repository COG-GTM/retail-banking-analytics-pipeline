"""Ticket 10 - Orchestration.

Replaces the shell orchestrators (``orchestration/run_full_pipeline.sh``,
``bteq/run_bteq_pipeline.sh``, ``sas/run_sas_pipeline.sh``) with a Python driver
that runs the ported jobs (tickets 4-10) in dependency order, using the shared
audit logging and validation from ticket 3. The same step functions back the
Databricks Workflow tasks defined in ``databricks/resources``.
"""
