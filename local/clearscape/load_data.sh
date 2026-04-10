#!/bin/bash
# Load synthetic data into ClearScape Teradata
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load credentials
if [ -f "${SCRIPT_DIR}/.env" ]; then
    source "${SCRIPT_DIR}/.env"
fi

echo "Generating synthetic data and loading to Teradata..."
echo "Host: ${TD_SERVER}"
echo "User: ${TD_USERNAME}"

# Run the Python data loader
uv run "${SCRIPT_DIR}/load_data_teradata.py" \
    --host "${TD_SERVER}" \
    --user "${TD_USERNAME}" \
    --password "${TD_PASSWORD}" \
    --customers 5000
