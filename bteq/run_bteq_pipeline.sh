#!/bin/bash
# =============================================================================
# BTEQ Pipeline Orchestrator -> PySpark compatibility shim
# =============================================================================
# The BTEQ staging layer has been migrated to PySpark (see ../staging/). The
# real driver is now staging/run_staging_pipeline.py; this thin wrapper is kept
# so the top-level orchestrator (orchestration/run_full_pipeline.sh), which
# still invokes bteq/run_bteq_pipeline.sh, continues to work unchanged.
#
# Usage:  ./run_bteq_pipeline.sh
# Exit:   0 = success, 99 = a stage produced zero rows, other = stage failure
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load shared pipeline configuration (LOOKBACK_MONTHS, RUN_TIMESTAMP, LOG_LEVEL, ...).
# shellcheck source=/dev/null
source "${REPO_ROOT}/config/pipeline_config.cfg"

PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "${REPO_ROOT}"
exec "${PYTHON_BIN}" -m staging.run_staging_pipeline "$@"
