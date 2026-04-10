#!/bin/bash
# =============================================================================
# End-to-End Pipeline Orchestrator
# =============================================================================
# Master script that runs the full data pipeline:
#   1. BTEQ layer  -> Teradata staging tables
#   2. SAS layer   -> Data product tables
#   3. Post-run    -> Validation and notification
#
# Usage:  ./run_full_pipeline.sh [--skip-bteq] [--skip-sas] [--dry-run]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config/pipeline_config.cfg"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
SKIP_BTEQ=false
SKIP_SAS=false
DRY_RUN=false

for arg in "$@"; do
    case ${arg} in
        --skip-bteq) SKIP_BTEQ=true ;;
        --skip-sas)  SKIP_SAS=true  ;;
        --dry-run)   DRY_RUN=true   ;;
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

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
log "============================================================"
log "  Retail Banking Analytics Pipeline"
log "  Run Date:    ${RUN_DATE}"
log "  Timestamp:   ${RUN_TIMESTAMP}"
log "  TD Server:   ${TD_SERVER}"
log "  Lookback:    ${LOOKBACK_MONTHS} months"
log "  Dry Run:     ${DRY_RUN}"
log "============================================================"

if [ "${DRY_RUN}" = true ]; then
    log "DRY RUN mode - listing steps only:"
    log "  1. BTEQ: 01_stg_customer_360 -> 02_stg_txn_summary -> 03_stg_risk_factors"
    log "  2. SAS:  01_customer_segments -> 02_txn_analytics -> 03_risk_scoring -> 04_data_products"
    log "  3. Post: Validation & notification"
    exit 0
fi

PIPELINE_START=$(date +%s)

# ---------------------------------------------------------------------------
# Phase 1: BTEQ Staging
# ---------------------------------------------------------------------------
if [ "${SKIP_BTEQ}" = false ]; then
    log "--- Phase 1: BTEQ Staging Layer ---"
    "${SCRIPT_DIR}/../bteq/run_bteq_pipeline.sh"
    BTEQ_RC=$?

    if [ ${BTEQ_RC} -ne 0 ]; then
        log "ABORT: BTEQ phase failed (rc=${BTEQ_RC}). SAS phase will not run."
        exit ${BTEQ_RC}
    fi
else
    log "--- Phase 1: BTEQ Staging Layer SKIPPED (--skip-bteq) ---"
fi

# ---------------------------------------------------------------------------
# Phase 2: SAS Analytics
# ---------------------------------------------------------------------------
if [ "${SKIP_SAS}" = false ]; then
    log "--- Phase 2: SAS Analytics Layer ---"
    "${SCRIPT_DIR}/../sas/run_sas_pipeline.sh"
    SAS_RC=$?

    if [ ${SAS_RC} -ne 0 ]; then
        log "ABORT: SAS phase failed (rc=${SAS_RC})."
        exit ${SAS_RC}
    fi
else
    log "--- Phase 2: SAS Analytics Layer SKIPPED (--skip-sas) ---"
fi

# ---------------------------------------------------------------------------
# Phase 3: Post-run validation
# ---------------------------------------------------------------------------
log "--- Phase 3: Post-Run Validation ---"

# Quick row-count validation via BTEQ
envsubst <<'BTEQ_EOF' | bteq >> "${MASTER_LOG}" 2>&1
.LOGON ${TD_SERVER}/${TD_USERNAME},;

SELECT 'CUSTOMER_SEGMENTS'     AS TBL, COUNT(*) AS ROWS FROM DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS
UNION ALL
SELECT 'TRANSACTION_ANALYTICS' AS TBL, COUNT(*) AS ROWS FROM DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS
UNION ALL
SELECT 'CUSTOMER_RISK_SCORES'  AS TBL, COUNT(*) AS ROWS FROM DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES
UNION ALL
SELECT 'CUSTOMER_MASTER_PROFILE' AS TBL, COUNT(*) AS ROWS FROM DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE
ORDER BY 1;

.LOGOFF;
.EXIT 0;
BTEQ_EOF

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
PIPELINE_END=$(date +%s)
ELAPSED=$(( PIPELINE_END - PIPELINE_START ))

log "============================================================"
log "  Pipeline Complete"
log "  Elapsed: $(( ELAPSED / 60 ))m $(( ELAPSED % 60 ))s"
log "  Master Log: ${MASTER_LOG}"
log "============================================================"

# Archive logs older than 30 days
find "${LOG_DIR}" -name "*.log" -mtime +30 -exec gzip {} \; 2>/dev/null || true

exit 0
