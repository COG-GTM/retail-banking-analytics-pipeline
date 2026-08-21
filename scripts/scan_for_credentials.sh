#!/bin/bash
# =============================================================================
# Credential Literal Scan - Retail Banking Customer Analytics
# Ticket: MBA-2203 (TICKET-02)
# =============================================================================
# Fails if any credential literal is committed to the repository. Run locally
# before pushing and from CI. Exits 0 when clean, 1 on the first finding.
#
# Usage: ./scan_for_credentials.sh [path]
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

# Patterns that indicate real credential material. A value that dereferences a
# variable ($VAR, &SAS_MACRO_VAR, {{template}}) is a runtime reference, not a
# literal, so those leading characters are excluded.
PATTERNS=(
    '\{SAS00[0-9]\}[A-Za-z0-9+/=]'                # SAS-encoded password blob
    'BEGIN [A-Z ]*PRIVATE KEY'                    # PEM private key body
    'password[[:space:]]*=[[:space:]]*["'\''][^"'\''$&#{]'   # literal password assignment
    'passwd[[:space:]]*=[[:space:]]*["'\''][^"'\''$&#{]'
    'pwd[[:space:]]*=[[:space:]]*["'\''][^"'\''$&#{]'
    'client_secret[[:space:]]*[=:][[:space:]]*["'\''][^"'\''$&#{]'
    'AKIA[0-9A-Z]{16}'                            # AWS access key id
    'sv=.*sig='                                   # Azure SAS token
)

EXCLUDES=(
    '--exclude-dir=.git'
    '--exclude-dir=data'
    '--exclude-dir=logs'
    "--exclude=$(basename "$0")"
)

echo "[scan] Scanning ${TARGET} for credential literals"

FOUND=0
for pattern in "${PATTERNS[@]}"; do
    if matches="$(grep -RInE "${EXCLUDES[@]}" -- "${pattern}" "${TARGET}" 2>/dev/null)"; then
        echo "[scan] FAIL: pattern /${pattern}/"
        echo "${matches}"
        FOUND=1
    fi
done

if [[ ${FOUND} -ne 0 ]]; then
    echo "[scan] Credential literals detected. Move the value into Azure Key Vault"
    echo "[scan] and reference it by secret name (see config/snowflake_config.cfg)."
    exit 1
fi

echo "[scan] PASS: no credential literals found"
exit 0
