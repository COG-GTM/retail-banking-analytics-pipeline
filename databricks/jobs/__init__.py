"""PySpark transformation logic for the migrated pipeline (tickets 4-10).

Each module exposes pure ``build_*`` function(s) that take input DataFrames and
return the output DataFrame, so the business logic is unit-testable without a
Databricks cluster. The notebooks in ``databricks/notebooks`` and the local
orchestrator in ``databricks/orchestration`` are thin wrappers that read/write
Delta tables around these functions.
"""
