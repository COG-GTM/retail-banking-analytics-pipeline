#!/bin/bash
# ClearScape Analytics Experience - Setup Script
# Configures your local environment to run BTEQ scripts against
# Teradata's free cloud sandbox.
#
# Prerequisites:
#   1. Sign up at https://clearscape.teradata.com (free, no credit card)
#   2. Docker installed (for teradata/bteq image)
#   3. Note your ClearScape host, username, and password
#
# Usage: ./setup.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../.."

# Check Docker
if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker is required. Install Docker Desktop first."
    exit 1
fi

# Pull BTEQ image
echo "Pulling teradata/bteq Docker image..."
docker pull teradata/bteq

# Prompt for credentials if not set
if [ -z "${TD_SERVER:-}" ]; then
    read -p "ClearScape hostname (e.g. host.clearscape.teradata.com): " TD_SERVER
    export TD_SERVER
fi
if [ -z "${TD_USERNAME:-}" ]; then
    read -p "ClearScape username: " TD_USERNAME
    export TD_USERNAME
fi
if [ -z "${TD_PASSWORD:-}" ]; then
    read -sp "ClearScape password: " TD_PASSWORD
    echo
    export TD_PASSWORD
fi

# Write a local .env file (gitignored)
cat > "${SCRIPT_DIR}/.env" <<EOF
export TD_SERVER="${TD_SERVER}"
export TD_USERNAME="${TD_USERNAME}"
export TD_PASSWORD="${TD_PASSWORD}"
export TD_LOGMECH="TD2"
export LOOKBACK_MONTHS=12
export RISK_SCORE_THRESHOLD=700
EOF
echo "Credentials saved to local/clearscape/.env (gitignored)"

# Test connection
echo "Testing connection to ${TD_SERVER}..."
echo ".LOGON ${TD_SERVER}/${TD_USERNAME},${TD_PASSWORD};
SELECT 'CONNECTION_OK' AS STATUS;
.LOGOFF;
.EXIT 0;" | docker run --rm -i teradata/bteq

echo ""
echo "Connection successful! Now creating databases..."

# Create databases (ClearScape may not allow CREATE DATABASE, so use CREATE SCHEMA or user's default DB)
# In ClearScape, the user typically gets their own database. We'll create schemas within it.
cat <<BTEQ_EOF | docker run --rm -i teradata/bteq
.LOGON ${TD_SERVER}/${TD_USERNAME},${TD_PASSWORD};

-- Create staging and data product tables in the user's default database
-- ClearScape uses the username as the default database

-- Create ETL Run Log table
CREATE MULTISET TABLE ETL_RUN_LOG, NO FALLBACK
(
    JOB_NAME    VARCHAR(60),
    STEP_NAME   VARCHAR(60),
    STATUS      VARCHAR(20),
    ROW_COUNT   INTEGER,
    START_TS    TIMESTAMP(6),
    END_TS      TIMESTAMP(6)
)
PRIMARY INDEX (JOB_NAME);

.IF ERRORCODE <> 0 THEN .GOTO LOG_EXISTS;
.LABEL LOG_EXISTS;

.LOGOFF;
.EXIT 0;
BTEQ_EOF

echo ""
echo "Setup complete! Next steps:"
echo "  1. Load synthetic data:  cd local/clearscape && ./load_data.sh"
echo "  2. Run BTEQ pipeline:   cd local/clearscape && ./run_pipeline.sh"
