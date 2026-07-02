#!/bin/bash
# =============================================================================
# Spark Pipeline Orchestrator
# =============================================================================
# PySpark replacement for sas/run_sas_pipeline.sh. Runs the four analytics jobs
# in sequence via spark/jobs/run_pipeline.py. Each job reads staging datasets
# (populated by BTEQ) and writes data-product datasets.
#
# Usage:  ./run_spark_pipeline.sh
# Exit:   0 = success, non-zero = failed job
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=/dev/null
source "${SCRIPT_DIR}/spark_pipeline.cfg"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SPARK] $1"
}

log "=========================================="
log "Spark Pipeline Start"
log "=========================================="

PYTHON_BIN="${PYTHON_BIN:-python}"

cd "${REPO_ROOT}"
"${PYTHON_BIN}" -m spark.jobs.run_pipeline

log "=========================================="
log "Spark Pipeline Complete"
log "=========================================="
