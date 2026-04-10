#!/bin/bash
# Run the actual BTEQ pipeline against ClearScape Teradata
#
# This script:
#   1. Rewrites database references (multi-DB -> single user DB)
#   2. Replaces .LOGON with ClearScape credentials
#   3. Runs each BTEQ script via the teradata/bteq Docker image
#   4. Runs the Python analytics (replacing SAS)
#
# Prerequisites: ./setup.sh and ./load_data.sh must have been run first.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../.."

# Load credentials
if [ -f "${SCRIPT_DIR}/.env" ]; then
    source "${SCRIPT_DIR}/.env"
fi

echo "============================================"
echo "  Running Pipeline Against ClearScape"
echo "  Host: ${TD_SERVER}"
echo "  User: ${TD_USERNAME}"
echo "============================================"

# Function to adapt and run a BTEQ script
run_bteq() {
    local script="$1"
    local script_name=$(basename "$script")
    echo ""
    echo "--- BTEQ: ${script_name} ---"

    # Adapt the script for ClearScape:
    # 1. Replace multi-database references with user's default DB
    # 2. Replace .LOGON line with actual credentials
    # 3. Remove COLLECT STATISTICS (optional on ClearScape free tier)
    # 4. Replace MONTHS_BETWEEN Teradata syntax if needed
    cat "${script}" \
        | sed "s/CORE_BANKING_DB\.//g" \
        | sed "s/TXN_PROCESSING_DB\.//g" \
        | sed "s/ETL_STAGING_DB\.//g" \
        | sed "s/DATA_PRODUCTS_DB\.//g" \
        | sed "s|\.LOGON \${TD_SERVER}/\${TD_USERNAME},;|.LOGON ${TD_SERVER}/${TD_USERNAME},${TD_PASSWORD};|g" \
        | sed "s/\.SET ERRORLEVEL 3807 SEVERITY 0;/.SET ERRORLEVEL 3807 SEVERITY 0;/" \
        | docker run --rm -i teradata/bteq

    local rc=$?
    if [ ${rc} -ne 0 ]; then
        echo "FAILED: ${script_name} (exit code: ${rc})"
        return ${rc}
    fi
    echo "SUCCESS: ${script_name}"
}

# Phase 1: Run BTEQ scripts
echo ""
echo "=== Phase 1: BTEQ Staging Layer ==="
run_bteq "${PROJECT_ROOT}/bteq/01_stg_customer_360.bteq"
run_bteq "${PROJECT_ROOT}/bteq/02_stg_txn_summary.bteq"
run_bteq "${PROJECT_ROOT}/bteq/03_stg_risk_factors.bteq"

# Phase 2: Run Python analytics (SAS replacement)
echo ""
echo "=== Phase 2: Python Analytics (SAS Replacement) ==="
uv run "${SCRIPT_DIR}/../python_sas/run_all.py" \
    --backend teradata \
    --host "${TD_SERVER}" \
    --user "${TD_USERNAME}" \
    --password "${TD_PASSWORD}"

echo ""
echo "============================================"
echo "  Pipeline Complete!"
echo "============================================"
