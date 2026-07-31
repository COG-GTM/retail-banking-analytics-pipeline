#!/bin/bash
# =============================================================================
# End-to-End Pipeline Orchestrator (dbt)
# =============================================================================
# Master script that runs the full data pipeline with dbt:
#   1. Staging layer -> ETL_STAGING_DB   (dbt run --select tag:staging tag:intermediate)
#   2. Marts layer   -> DATA_PRODUCTS_DB (dbt run --select tag:marts)
#   3. Post-run      -> dbt test + row-count summary
#
# The BTEQ and SAS runners this script used to call are deprecated; see
# bteq/run_bteq_pipeline.sh and sas/run_sas_pipeline.sh.
#
# Usage:  ./run_full_pipeline.sh [--skip-bteq] [--skip-sas] [--dry-run]
#
#   --skip-bteq   Skip the staging layer, build marts only (was: skip BTEQ)
#   --skip-sas    Build the staging layer only, skip marts (was: skip SAS)
#   --dry-run     Print the dbt commands without executing them
#
# Environment: TD_PASSWORD must be exported before running; the remaining
# connection settings come from config/pipeline_config.cfg.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config/pipeline_config.cfg"

DBT_DIR="${DBT_PROJECT_DIR:-${SCRIPT_DIR}/../dbt}"
DBT_BIN="${DBT_BIN:-dbt}"
DBT_TARGET="${DBT_TARGET:-dev}"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
SKIP_STAGING=false
SKIP_MARTS=false
DRY_RUN=false

for arg in "$@"; do
    case ${arg} in
        --skip-bteq) SKIP_STAGING=true ;;
        --skip-sas)  SKIP_MARTS=true   ;;
        --dry-run)   DRY_RUN=true      ;;
        *)           echo "Unknown argument: ${arg}"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
mkdir -p "${LOG_DIR}"
MASTER_LOG="${LOG_DIR}/pipeline_master_${RUN_TIMESTAMP}.log"
exec > >(tee -a "${MASTER_LOG}") 2>&1

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MASTER] $1"
}

dbt_run() {
    # Usage: dbt_run <subcommand> [args...]
    local cmd=("${DBT_BIN}" "$@"
               --project-dir "${DBT_DIR}"
               --profiles-dir "${DBT_PROFILES_DIR:-${DBT_DIR}}"
               --target "${DBT_TARGET}")

    if [ "${DRY_RUN}" = true ]; then
        log "DRY RUN: ${cmd[*]}"
        return 0
    fi

    log "EXEC: ${cmd[*]}"
    "${cmd[@]}"
}

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
log "============================================================"
log "  Retail Banking Analytics Pipeline (dbt)"
log "  Run Date:    ${RUN_DATE}"
log "  Timestamp:   ${RUN_TIMESTAMP}"
log "  TD Server:   ${TD_SERVER}"
log "  dbt Target:  ${DBT_TARGET}"
log "  Lookback:    ${LOOKBACK_MONTHS} months"
log "  Dry Run:     ${DRY_RUN}"
log "============================================================"

if [ "${DRY_RUN}" = false ] && [ -z "${TD_PASSWORD:-}" ]; then
    log "ABORT: TD_PASSWORD is not set; export it before running the pipeline."
    exit 2
fi

PIPELINE_START=$(date +%s)

# ---------------------------------------------------------------------------
# Phase 0: Dependencies and hand-off seeds
# ---------------------------------------------------------------------------
log "--- Phase 0: dbt deps + seeds ---"
dbt_run deps
dbt_run seed

# ---------------------------------------------------------------------------
# Phase 1: Staging layer (formerly the BTEQ scripts)
# ---------------------------------------------------------------------------
if [ "${SKIP_STAGING}" = false ]; then
    log "--- Phase 1: Staging Layer (tag:staging tag:intermediate) ---"
    dbt_run run --select tag:staging tag:intermediate
else
    log "--- Phase 1: Staging Layer SKIPPED (--skip-bteq) ---"
fi

# ---------------------------------------------------------------------------
# Phase 2: Marts layer (formerly the SAS programs)
# ---------------------------------------------------------------------------
if [ "${SKIP_MARTS}" = false ]; then
    log "--- Phase 2: Marts Layer (tag:marts) ---"
    log "NOTE: run the scoring hand-offs before this phase if the k-means or"
    log "      probability-of-default outputs need refreshing:"
    log "      dbt run-operation run_td_kmeans_segments"
    log "      dbt run-operation run_td_glm_default_scores"
    dbt_run run --select tag:marts
else
    log "--- Phase 2: Marts Layer SKIPPED (--skip-sas) ---"
fi

# ---------------------------------------------------------------------------
# Phase 3: Tests (replaces the SAS %validate_table checks)
# ---------------------------------------------------------------------------
log "--- Phase 3: dbt test ---"
if [ "${SKIP_STAGING}" = true ]; then
    dbt_run test --select tag:marts
elif [ "${SKIP_MARTS}" = true ]; then
    dbt_run test --select tag:staging tag:intermediate
else
    dbt_run test
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
PIPELINE_END=$(date +%s)
ELAPSED=$(( PIPELINE_END - PIPELINE_START ))

log "============================================================"
log "  Pipeline Complete"
log "  Elapsed: $(( ELAPSED / 60 ))m $(( ELAPSED % 60 ))s"
log "  Master Log:   ${MASTER_LOG}"
log "  Run Results:  ${DBT_DIR}/target/run_results.json"
log "  Audit Table:  ${DB_STG}.ETL_RUN_LOG (written by the on-run-end hook)"
log "============================================================"

# Archive logs older than 30 days
find "${LOG_DIR}" -name "*.log" -mtime +30 -exec gzip {} \; 2>/dev/null || true

exit 0
