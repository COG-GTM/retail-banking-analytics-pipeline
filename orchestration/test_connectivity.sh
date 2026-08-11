#!/bin/bash
# =============================================================================
# Teradata Connectivity Smoke Test
# =============================================================================
# Validates that every database the pipeline reads from or writes to is
# reachable and returns data from the endpoint currently configured in
# TD_SERVER. Intended as a pre-cutover gate when repointing the pipeline at
# the migrated cloud Teradata (Vantage) endpoint:
#
#   TD_SERVER=<cloud-host> TD_LOGMECH=TD2 ./orchestration/test_connectivity.sh
#
# Usage:  ./test_connectivity.sh [--dry-run]
# Exit:   0 = all checks passed, non-zero = at least one check failed
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config/pipeline_config.cfg"

DRY_RUN=false
for arg in "$@"; do
    case ${arg} in
        --dry-run) DRY_RUN=true ;;
        *)         echo "Unknown argument: ${arg}"; exit 1 ;;
    esac
done

mkdir -p "${LOG_DIR}"
TEST_LOG="${LOG_DIR}/connectivity_test_${RUN_TIMESTAMP}.log"
exec > >(tee -a "${TEST_LOG}") 2>&1

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CONNTEST] $1"
}

log "============================================================"
log "  Teradata Connectivity Smoke Test"
log "  TD Server:   ${TD_SERVER}"
log "  TD Username: ${TD_USERNAME}"
log "  TD Logmech:  ${TD_LOGMECH}"
log "  Run Date:    ${RUN_DATE}"
log "  Log File:    ${TEST_LOG}"
log "============================================================"

if [ "${DRY_RUN}" = true ]; then
    log "DRY RUN mode - listing checks only:"
    log "  1. BTEQ logon to ${TD_SERVER} as ${TD_USERNAME} (${TD_LOGMECH})"
    log "  2. SELECT 1 against ${DB_CORE}, ${DB_TXN}, ${DB_STG}, ${DB_DP}"
    log "  3. SELECT COUNT(*) against ${DB_CORE}.CUSTOMERS and ${DB_TXN}.TRANSACTIONS"
    exit 0
fi

FAILURES=0

# ---------------------------------------------------------------------------
# run_bteq_check <label> <bteq-body>
#   Wraps a BTEQ body with logon/logoff, runs it, and inspects both the exit
#   code and the log for BTEQ-level failures (BTEQ can exit 0 on some errors).
# ---------------------------------------------------------------------------
run_bteq_check() {
    local label="$1"
    local body="$2"
    local log_file="${LOG_DIR}/connectivity_${label}_${RUN_TIMESTAMP}.log"
    local rc=0

    log "START: ${label}"

    bteq > "${log_file}" 2>&1 <<BTEQ_EOF || rc=$?
.SET WIDTH 254;
.SET ERRORLEVEL 3807 SEVERITY 8;
.LOGON ${TD_SERVER}/${TD_USERNAME},;
.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

${body}

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;
.LOGOFF;
.EXIT 0;
BTEQ_EOF

    if [ ${rc} -ne 0 ]; then
        log "FAILED: ${label} (exit code: ${rc}). See ${log_file}"
        FAILURES=$(( FAILURES + 1 ))
        return 0
    fi

    if grep -qiE '^\*\*\* Failure|^\*\*\* Error|^\*\*\* Warning: RDBMS' "${log_file}"; then
        log "FAILED: ${label} (BTEQ error detected in log). See ${log_file}"
        grep -iE '^\*\*\* Failure|^\*\*\* Error' "${log_file}" | head -5
        FAILURES=$(( FAILURES + 1 ))
        return 0
    fi

    log "SUCCESS: ${label}"
    return 0
}

# ---------------------------------------------------------------------------
# Check 1: logon + SELECT 1 against each database
# ---------------------------------------------------------------------------
log "--- Check 1: Logon and SELECT 1 per database ---"

for db in "${DB_CORE}" "${DB_TXN}" "${DB_STG}" "${DB_DP}"; do
    run_bteq_check "select1_${db}" "DATABASE ${db};
SELECT 1;"
done

# ---------------------------------------------------------------------------
# Check 2: row counts against a known table in each source database
# ---------------------------------------------------------------------------
log "--- Check 2: Row counts against known tables ---"

run_bteq_check "rowcount_core" "SELECT '${DB_CORE}.CUSTOMERS' AS TBL, COUNT(*) AS ROW_CNT FROM ${DB_CORE}.CUSTOMERS;"
run_bteq_check "rowcount_txn"  "SELECT '${DB_TXN}.TRANSACTIONS' AS TBL, COUNT(*) AS ROW_CNT FROM ${DB_TXN}.TRANSACTIONS;"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log "============================================================"
if [ ${FAILURES} -ne 0 ]; then
    log "  RESULT: FAILED - ${FAILURES} check(s) failed against ${TD_SERVER}"
    log "  Log File: ${TEST_LOG}"
    log "============================================================"
    exit 1
fi

log "  RESULT: PASSED - all checks succeeded against ${TD_SERVER}"
log "  Next: run 'sas/test_connectivity.sas' and 'orchestration/compare_parity.sh'"
log "  Log File: ${TEST_LOG}"
log "============================================================"
exit 0
