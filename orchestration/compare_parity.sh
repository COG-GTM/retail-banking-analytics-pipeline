#!/bin/bash
# =============================================================================
# Data-Product Row-Count Parity Comparison
# =============================================================================
# Runs the same row-count query used by the post-run validation phase of
# run_full_pipeline.sh against two Teradata endpoints (the legacy on-prem host
# and the migrated cloud host) and diffs the results, so a cutover can be
# gated on the data products matching.
#
# Usage:
#   TD_SERVER=<cloud-host> TD_SERVER_LEGACY=<onprem-host> \
#       ./compare_parity.sh [--legacy <host>] [--cloud <host>] [--dry-run]
#
# Exit:   0 = counts match, 1 = mismatch or query failure
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config/pipeline_config.cfg"

LEGACY_SERVER="${TD_SERVER_LEGACY}"
CLOUD_SERVER="${TD_SERVER}"
DRY_RUN=false

while [ $# -gt 0 ]; do
    case "$1" in
        --legacy) LEGACY_SERVER="$2"; shift 2 ;;
        --cloud)  CLOUD_SERVER="$2";  shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

mkdir -p "${LOG_DIR}"
PARITY_LOG="${LOG_DIR}/parity_${RUN_TIMESTAMP}.log"
exec > >(tee -a "${PARITY_LOG}") 2>&1

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [PARITY] $1"
}

log "============================================================"
log "  Data Product Row-Count Parity Check"
log "  Legacy Server: ${LEGACY_SERVER}"
log "  Cloud Server:  ${CLOUD_SERVER}"
log "  TD Username:   ${TD_USERNAME}"
log "  TD Logmech:    ${TD_LOGMECH}"
log "============================================================"

if [ "${LEGACY_SERVER}" = "${CLOUD_SERVER}" ]; then
    log "ABORT: legacy and cloud servers are identical (${CLOUD_SERVER})."
    log "       Set TD_SERVER/TD_SERVER_LEGACY or pass --legacy/--cloud."
    exit 1
fi

if [ "${DRY_RUN}" = true ]; then
    log "DRY RUN mode - would compare row counts for:"
    log "  ${DB_DP}.CUSTOMER_SEGMENTS, TRANSACTION_ANALYTICS,"
    log "  CUSTOMER_RISK_SCORES, CUSTOMER_MASTER_PROFILE"
    exit 0
fi

# ---------------------------------------------------------------------------
# collect_counts <server> <output-file>
#   Emits "TABLE|COUNT" lines for the four data product tables.
# ---------------------------------------------------------------------------
collect_counts() {
    local server="$1"
    local out_file="$2"
    local raw_log="${LOG_DIR}/parity_${server}_${RUN_TIMESTAMP}.log"
    local rc=0

    log "Collecting counts from ${server}"

    bteq > "${raw_log}" 2>&1 <<BTEQ_EOF || rc=$?
.SET WIDTH 254;
.SET TITLEDASHES OFF;
.LOGON ${server}/${TD_USERNAME},;
.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

SELECT 'CUSTOMER_SEGMENTS'       (CHAR(24)) || '|' || TRIM(CAST(COUNT(*) AS INTEGER)) FROM ${DB_DP}.CUSTOMER_SEGMENTS
UNION ALL
SELECT 'TRANSACTION_ANALYTICS'   (CHAR(24)) || '|' || TRIM(CAST(COUNT(*) AS INTEGER)) FROM ${DB_DP}.TRANSACTION_ANALYTICS
UNION ALL
SELECT 'CUSTOMER_RISK_SCORES'    (CHAR(24)) || '|' || TRIM(CAST(COUNT(*) AS INTEGER)) FROM ${DB_DP}.CUSTOMER_RISK_SCORES
UNION ALL
SELECT 'CUSTOMER_MASTER_PROFILE' (CHAR(24)) || '|' || TRIM(CAST(COUNT(*) AS INTEGER)) FROM ${DB_DP}.CUSTOMER_MASTER_PROFILE
ORDER BY 1;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;
.LOGOFF;
.EXIT 0;
BTEQ_EOF

    if [ ${rc} -ne 0 ] || grep -qiE '^\*\*\* Failure|^\*\*\* Error' "${raw_log}"; then
        log "FAILED: row-count query against ${server} (rc=${rc}). See ${raw_log}"
        return 1
    fi

    grep -oE '[A-Z_]+ *\|[0-9]+' "${raw_log}" \
        | tr -d ' ' \
        | sort -u > "${out_file}"

    if [ ! -s "${out_file}" ]; then
        log "FAILED: no row counts parsed from ${server}. See ${raw_log}"
        return 1
    fi

    return 0
}

LEGACY_COUNTS="${LOG_DIR}/parity_legacy_${RUN_TIMESTAMP}.counts"
CLOUD_COUNTS="${LOG_DIR}/parity_cloud_${RUN_TIMESTAMP}.counts"

collect_counts "${LEGACY_SERVER}" "${LEGACY_COUNTS}"
collect_counts "${CLOUD_SERVER}"  "${CLOUD_COUNTS}"

# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------
log "--- Row counts (legacy vs cloud) ---"
printf '%-26s %14s %14s %s\n' "TABLE" "LEGACY" "CLOUD" "STATUS"

MISMATCHES=0
while IFS='|' read -r tbl legacy_cnt; do
    cloud_cnt="$(awk -F'|' -v t="${tbl}" '$1 == t { print $2 }' "${CLOUD_COUNTS}")"
    if [ -z "${cloud_cnt}" ]; then
        cloud_cnt="MISSING"
    fi

    if [ "${legacy_cnt}" = "${cloud_cnt}" ]; then
        status="MATCH"
    else
        status="MISMATCH"
        MISMATCHES=$(( MISMATCHES + 1 ))
    fi
    printf '%-26s %14s %14s %s\n' "${tbl}" "${legacy_cnt}" "${cloud_cnt}" "${status}"
done < "${LEGACY_COUNTS}"

log "============================================================"
if [ ${MISMATCHES} -ne 0 ]; then
    log "  RESULT: FAILED - ${MISMATCHES} table(s) differ between endpoints"
    log "  Log File: ${PARITY_LOG}"
    log "============================================================"
    exit 1
fi

log "  RESULT: PASSED - all data product row counts match"
log "  Log File: ${PARITY_LOG}"
log "============================================================"
exit 0
