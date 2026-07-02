#!/bin/bash
# =============================================================================
# End-to-End Pipeline Orchestrator (Spark / Delta Lake)
# =============================================================================
# Master script that submits the full data pipeline as Spark jobs:
#   0. DDL bootstrap -> create Delta databases + tables (spark/apply_ddl.py)
#   1. Staging layer -> Delta staging tables   (spark/staging/*.py)
#   2. Products layer-> Delta data products    (spark/data_products/*.py)
#   3. Post-run      -> row-count validation   (spark/validate_products.py)
#
# The staging (BTEQ->PySpark) and data-product (SAS->PySpark) jobs are migrated
# under separate tickets; this orchestrator references them by their agreed
# names/paths so it can be run once those jobs land.
#
# Usage:
#   ./run_full_pipeline.sh [--skip-ddl] [--skip-staging|--skip-bteq] \
#                          [--skip-products|--skip-sas] [--dry-run]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=/dev/null
source "${REPO_ROOT}/config/pipeline_config.cfg"

# Job locations (repo-relative; override via env for a deployed layout).
SPARK_DIR="${SPARK_DIR:-${REPO_ROOT}/spark}"
DDL_DIR="${DDL_DIR:-${REPO_ROOT}/ddl}"
export DDL_DIR
SPARK_SESSION_PY="${SPARK_DIR}/spark_session.py"
DDL_BOOTSTRAP="${SPARK_DIR}/apply_ddl.py"
VALIDATE_SCRIPT="${SPARK_DIR}/validate_products.py"

STAGING_JOBS=(
    "staging/01_stg_customer_360.py"
    "staging/02_stg_txn_summary.py"
    "staging/03_stg_risk_factors.py"
)
PRODUCT_JOBS=(
    "data_products/01_customer_segments.py"
    "data_products/02_transaction_analytics.py"
    "data_products/03_risk_scoring.py"
    "data_products/04_data_products.py"
)

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
SKIP_DDL=false
SKIP_STAGING=false
SKIP_PRODUCTS=false
DRY_RUN=false

for arg in "$@"; do
    case ${arg} in
        --skip-ddl)                 SKIP_DDL=true ;;
        --skip-staging|--skip-bteq) SKIP_STAGING=true ;;
        --skip-products|--skip-sas) SKIP_PRODUCTS=true ;;
        --dry-run)                  DRY_RUN=true ;;
        *) echo "Unknown argument: ${arg}" >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
mkdir -p "${LOG_DIR}"
MASTER_LOG="${LOG_DIR}/pipeline_master_${RUN_TIMESTAMP}.log"
exec > >(tee -a "${MASTER_LOG}") 2>&1

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MASTER] [$1] ${2}"
}

die() {
    log "ERROR" "$1"
    exit "${2:-1}"
}

# ---------------------------------------------------------------------------
# spark-submit wrapper: submits a job and fails fast on non-zero exit.
# ---------------------------------------------------------------------------
spark_submit() {
    local job_path="$1"; shift
    local job_name
    job_name="$(basename "${job_path}" .py)"

    if [ ! -f "${job_path}" ]; then
        die "Spark job not found: ${job_path}" 3
    fi

    log "INFO" "Submitting Spark job: ${job_name}"
    # shellcheck disable=SC2086 - SPARK_EXTRA_CONF is intentionally word-split
    "${SPARK_SUBMIT}" \
        --master "${SPARK_MASTER}" \
        --deploy-mode "${SPARK_DEPLOY_MODE}" \
        --name "${SPARK_APP_NAME}_${job_name}" \
        --packages "${SPARK_DELTA_PACKAGE}" \
        --py-files "${SPARK_SESSION_PY}" \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --conf "spark.sql.warehouse.dir=${WAREHOUSE_LOCATION}" \
        ${SPARK_EXTRA_CONF} \
        "${job_path}" "$@"
}

run_job_group() {
    local group="$1"; shift
    local rel
    for rel in "$@"; do
        if ! spark_submit "${SPARK_DIR}/${rel}"; then
            die "${group} job failed: ${rel}" 1
        fi
    done
}

# ---------------------------------------------------------------------------
# Pre-flight banner
# ---------------------------------------------------------------------------
log "INFO" "============================================================"
log "INFO" "  Retail Banking Analytics Pipeline (Spark / Delta)"
log "INFO" "  Run Date:    ${RUN_DATE}"
log "INFO" "  Timestamp:   ${RUN_TIMESTAMP}"
log "INFO" "  Spark Master:${SPARK_MASTER}"
log "INFO" "  Warehouse:   ${WAREHOUSE_LOCATION}"
log "INFO" "  Lookback:    ${LOOKBACK_MONTHS} months"
log "INFO" "  Dry Run:     ${DRY_RUN}"
log "INFO" "============================================================"

if [ "${DRY_RUN}" = true ]; then
    log "INFO" "DRY RUN - listing steps only:"
    log "INFO" "  0. DDL:      apply_ddl.py (databases + Delta tables)"
    log "INFO" "  1. Staging:  ${STAGING_JOBS[*]}"
    log "INFO" "  2. Products: ${PRODUCT_JOBS[*]}"
    log "INFO" "  3. Validate: validate_products.py"
    exit 0
fi

PIPELINE_START=$(date +%s)

# ---------------------------------------------------------------------------
# Phase 0: DDL bootstrap
# ---------------------------------------------------------------------------
if [ "${SKIP_DDL}" = false ]; then
    log "INFO" "--- Phase 0: DDL Bootstrap ---"
    spark_submit "${DDL_BOOTSTRAP}" || die "DDL bootstrap failed." 1
else
    log "INFO" "--- Phase 0: DDL Bootstrap SKIPPED (--skip-ddl) ---"
fi

# ---------------------------------------------------------------------------
# Phase 1: Staging layer
# ---------------------------------------------------------------------------
if [ "${SKIP_STAGING}" = false ]; then
    log "INFO" "--- Phase 1: Staging Layer ---"
    run_job_group "Staging" "${STAGING_JOBS[@]}"
else
    log "INFO" "--- Phase 1: Staging Layer SKIPPED (--skip-staging) ---"
fi

# ---------------------------------------------------------------------------
# Phase 2: Data-product layer
# ---------------------------------------------------------------------------
if [ "${SKIP_PRODUCTS}" = false ]; then
    log "INFO" "--- Phase 2: Data-Product Layer ---"
    run_job_group "Products" "${PRODUCT_JOBS[@]}"
else
    log "INFO" "--- Phase 2: Data-Product Layer SKIPPED (--skip-products) ---"
fi

# ---------------------------------------------------------------------------
# Phase 3: Post-run validation (replaces the inline BTEQ row-count block)
# ---------------------------------------------------------------------------
log "INFO" "--- Phase 3: Post-Run Validation ---"
spark_submit "${VALIDATE_SCRIPT}" || die "Post-run validation failed." 1

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
PIPELINE_END=$(date +%s)
ELAPSED=$(( PIPELINE_END - PIPELINE_START ))

log "INFO" "============================================================"
log "INFO" "  Pipeline Complete"
log "INFO" "  Elapsed: $(( ELAPSED / 60 ))m $(( ELAPSED % 60 ))s"
log "INFO" "  Master Log: ${MASTER_LOG}"
log "INFO" "============================================================"

# Archive logs older than 30 days
find "${LOG_DIR}" -name "*.log" -mtime +30 -exec gzip {} \; 2>/dev/null || true

exit 0
