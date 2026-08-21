#!/bin/bash
# =============================================================================
# Snowflake Environment Provisioner - Retail Banking Customer Analytics
# Ticket: MBA-2203 (TICKET-02)
# =============================================================================
# Applies the warehouse / role / grant / service-principal model in order and
# then verifies least privilege. Every script is re-runnable, so this runner is
# safe to execute on every deployment.
#
#   00_resource_monitors.sql     credit quotas and suspend triggers
#   01_warehouses.sql            per-workload warehouses
#   02_roles.sql                 functional roles and hierarchy
#   03_grants.sql                least-privilege grant matrix
#   04_service_principals.sql    key-pair service users + Entra OAuth
#   05_verify_least_privilege.sql assertions
#
# Credentials: the admin connection authenticates with a key pair whose private
# key is pulled from Azure Key Vault at runtime. No password is read from, or
# written to, this repository.
#
# Usage: ./run_snowflake_admin.sh [--env DEV|UAT|PROD] [--dry-run] [--skip-verify]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
DRY_RUN=false
SKIP_VERIFY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --env)          export SF_ENV="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --skip-verify)  SKIP_VERIFY=true; shift ;;
        *)              echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# shellcheck source=../../config/snowflake_config.cfg
source "${REPO_ROOT}/config/snowflake_config.cfg"
# shellcheck source=../../scripts/keyvault.sh
source "${REPO_ROOT}/scripts/keyvault.sh"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SF-ADMIN] $1"
}

trap kv_cleanup EXIT

# ---------------------------------------------------------------------------
# Resolve credentials from Azure Key Vault
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "true" ]]; then
    log "DRY RUN - no Key Vault access, no statements executed"
    ADMIN_KEY_FILE="/dev/null"
    ADMIN_KEY_PASSPHRASE=""
    SVC_SYNAPSE_PUBKEY="DRY_RUN"
    SVC_LOADER_PUBKEY="DRY_RUN"
else
    kv_require_login
    log "Reading credentials from Key Vault ${AZ_KEY_VAULT_NAME}"
    ADMIN_KEY_FILE="$(kv_get_private_key "${KV_SECRET_ADMIN_PRIVATE_KEY}")"
    ADMIN_KEY_PASSPHRASE="$(kv_get_secret "${KV_SECRET_ADMIN_KEY_PASSPHRASE}")"
    SVC_SYNAPSE_PUBKEY="$(kv_get_secret "${KV_SECRET_SYNAPSE_PUBLIC_KEY}")"
    SVC_LOADER_PUBKEY="$(kv_get_secret "${KV_SECRET_LOADER_PUBLIC_KEY}")"
fi

# ---------------------------------------------------------------------------
# Execute a SQL script with the shared variable set
# ---------------------------------------------------------------------------
run_sql() {
    local sql_file="$1"

    log "Applying $(basename "${sql_file}")"

    if [[ "${DRY_RUN}" == "true" ]]; then
        log "  (dry run) snowsql -f ${sql_file}"
        return 0
    fi

    SNOWSQL_PRIVATE_KEY_PASSPHRASE="${ADMIN_KEY_PASSPHRASE}" \
    snowsql \
        --accountname "${SF_ACCOUNT}" \
        --username "SVC_RB_ADMIN_${SF_ENV}" \
        --private-key-path "${ADMIN_KEY_FILE}" \
        --authenticator SNOWFLAKE_JWT \
        --rolename ACCOUNTADMIN \
        --warehousename "${SF_WH_ELT}" \
        --option exit_on_error=true \
        --option friendly=false \
        --option variable_substitution=true \
        --variable env="${SF_ENV}" \
        --variable db_core="${SF_DB_CORE}" \
        --variable db_txn="${SF_DB_TXN}" \
        --variable db_stg="${SF_DB_STG}" \
        --variable db_dp="${SF_DB_DP}" \
        --variable quota_elt="${SF_QUOTA_ELT}" \
        --variable quota_spark="${SF_QUOTA_SPARK}" \
        --variable quota_adhoc="${SF_QUOTA_ADHOC}" \
        --variable svc_synapse_user="${SF_SVC_SYNAPSE_USER}" \
        --variable svc_loader_user="${SF_SVC_LOADER_USER}" \
        --variable svc_synapse_pubkey="${SVC_SYNAPSE_PUBKEY}" \
        --variable svc_loader_pubkey="${SVC_LOADER_PUBKEY}" \
        --variable entra_tenant_id="${AZ_TENANT_ID}" \
        --filename "${sql_file}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "Environment: ${SF_ENV} | account: ${SF_ACCOUNT}"

for sql_file in \
    "${SCRIPT_DIR}/00_resource_monitors.sql" \
    "${SCRIPT_DIR}/01_warehouses.sql" \
    "${SCRIPT_DIR}/02_roles.sql" \
    "${SCRIPT_DIR}/03_grants.sql" \
    "${SCRIPT_DIR}/04_service_principals.sql"
do
    run_sql "${sql_file}"
done

if [[ "${SKIP_VERIFY}" == "true" ]]; then
    log "Verification skipped (--skip-verify)"
else
    run_sql "${SCRIPT_DIR}/05_verify_least_privilege.sql"
fi

log "Snowflake environment provisioning complete for ${SF_ENV}"
