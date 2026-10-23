# synapse/pipelines — Orchestration (Ticket T10)

**Owner:** [T10](../tickets/TICKET-10.md). No other ticket writes here.

Synapse workspace artifacts in the Synapse Git-integration folder layout
(`pipeline/`, `trigger/`, `linkedService/`, `sparkJobDefinition/`, `dataset/`) plus the
deployment helper that packages `synapse/common/` as `common.zip`. Replaces
`orchestration/run_full_pipeline.sh`, `bteq/run_bteq_pipeline.sh`, `sas/run_sas_pipeline.sh`
and the 02:00 ET cron entry in `docs/pipeline_flow.md`.

Activity targets (proc names, Spark job files, parameters) are frozen in
[`../common/contracts/07_entry_points_contract.md`](../common/contracts/07_entry_points_contract.md).
