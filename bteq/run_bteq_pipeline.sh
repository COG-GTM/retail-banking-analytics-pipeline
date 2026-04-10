#!/bin/bash
# =============================================================================
# BTEQ Pipeline Orchestrator
# =============================================================================
# Runs the three BTEQ staging scripts in sequence with error handling.
# Each script feeds downstream SAS programs.
#
# Usage:  ./run_bteq_pipeline.sh
# Exit:   0 = success, non-zero = step that failed
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config/pipeline_config.cfg"

mkdir -p "${LOG_DIR}/bteq"

# ---------------------------------------------------------------------------
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [BTEQ] $1"
}

run_bteq() {
    local script_name="$1"
    local script_path="${BTEQ_DIR}/${script_name}"
    local log_file="${LOG_DIR}/bteq/${script_name%.bteq}_${RUN_TIMESTAMP}.log"

    log "START: ${script_name}"

    # Substitute environment variables in the BTEQ script and execute
    envsubst < "${script_path}" | bteq > "${log_file}" 2>&1
    local rc=$?

    if [ ${rc} -ne 0 ]; then
        log "FAILED: ${script_name} (exit code: ${rc}). See ${log_file}"
        return ${rc}
    fi

    # Check for BTEQ-level failures in the log
    if grep -qiE '^\*\*\* Failure|^\*\*\* Error' "${log_file}"; then
        log "FAILED: ${script_name} (BTEQ error detected in log). See ${log_file}"
        return 1
    fi

    log "SUCCESS: ${script_name}"
    return 0
}

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
log "=========================================="
log "BTEQ Pipeline Start - Run: ${RUN_TIMESTAMP}"
log "=========================================="

STEPS=(
    "01_stg_customer_360.bteq"
    "02_stg_txn_summary.bteq"
    "03_stg_risk_factors.bteq"
)

for step in "${STEPS[@]}"; do
    run_bteq "${step}"
done

log "=========================================="
log "BTEQ Pipeline Complete"
log "=========================================="
exit 0
