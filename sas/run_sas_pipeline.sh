#!/bin/bash
# =============================================================================
# SAS Pipeline Orchestrator
# =============================================================================
# Runs the four SAS programs in sequence. Each program reads from staging
# tables (populated by BTEQ) and writes to data product tables.
#
# Usage:  ./run_sas_pipeline.sh
# Exit:   0 = success, non-zero = step that failed
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config/pipeline_config.cfg"

mkdir -p "${LOG_DIR}/sas"

# ---------------------------------------------------------------------------
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SAS] $1"
}

run_sas() {
    local program_name="$1"
    local program_path="${SAS_DIR}/${program_name}"
    local log_file="${LOG_DIR}/sas/${program_name%.sas}_${RUN_TIMESTAMP}.log"
    local lst_file="${LOG_DIR}/sas/${program_name%.sas}_${RUN_TIMESTAMP}.lst"

    log "START: ${program_name}"

    "${SAS_BATCH}" \
        -sysin "${program_path}" \
        -log "${log_file}" \
        -print "${lst_file}" \
        -autoexec "${SAS_AUTOEXEC}" \
        -config "${SAS_CONFIG}/sasv9.cfg" \
        -memsize 4G \
        -sortsize 2G \
        -noovp \
        -nosyntaxcheck

    local rc=$?

    if [ ${rc} -ne 0 ]; then
        # SAS exit codes: 0=clean, 1=warnings, 2+=errors
        if [ ${rc} -ge 2 ]; then
            log "FAILED: ${program_name} (SAS exit code: ${rc}). See ${log_file}"
            return ${rc}
        else
            log "WARNING: ${program_name} completed with warnings (rc=${rc})"
        fi
    fi

    # Check for ERROR lines in the SAS log
    local error_count
    error_count=$(grep -c '^ERROR' "${log_file}" 2>/dev/null || echo 0)
    if [ "${error_count}" -gt 0 ]; then
        log "FAILED: ${program_name} (${error_count} ERROR(s) in SAS log). See ${log_file}"
        return 2
    fi

    log "SUCCESS: ${program_name}"
    return 0
}

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
log "=========================================="
log "SAS Pipeline Start - Run: ${RUN_TIMESTAMP}"
log "=========================================="

PROGRAMS=(
    "01_sas_customer_segments.sas"
    "02_sas_txn_analytics.sas"
    "03_sas_risk_scoring.sas"
    "04_sas_data_products.sas"
)

for prog in "${PROGRAMS[@]}"; do
    run_sas "${prog}"
done

log "=========================================="
log "SAS Pipeline Complete"
log "=========================================="
exit 0
