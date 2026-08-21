#!/bin/bash
# =============================================================================
# Snowflake Key-Pair Rotation - Retail Banking Customer Analytics
# Ticket: MBA-2203 (TICKET-02)
# =============================================================================
# Zero-downtime rotation using Snowflake's dual key slots (RSA_PUBLIC_KEY and
# RSA_PUBLIC_KEY_2): the new key is published to slot 2 while slot 1 stays
# valid, callers are cut over, then slot 1 is replaced and slot 2 cleared.
#
# Phases:
#   generate   create a new key pair, store it in Key Vault as a NEW version
#   publish    register the new public key in the user's second key slot
#   cutover    point Key Vault's "current" secret at the new private key
#   retire     move the new key into slot 1 and unset slot 2
#
# Usage:
#   ./rotate_snowflake_key.sh --user synapse --phase generate
#   ./rotate_snowflake_key.sh --user synapse --phase publish
#   ./rotate_snowflake_key.sh --user synapse --phase cutover
#   ./rotate_snowflake_key.sh --user synapse --phase retire
#
#   --user   synapse | loader | admin
#   --phase  generate | publish | cutover | retire
#   --dry-run
#
# Documented end to end in docs/modernization/snowflake_security.md.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_USER=""
PHASE=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --user)    TARGET_USER="$2"; shift 2 ;;
        --phase)   PHASE="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *)         echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# shellcheck source=../config/snowflake_config.cfg
source "${REPO_ROOT}/config/snowflake_config.cfg"
# shellcheck source=./keyvault.sh
source "${REPO_ROOT}/scripts/keyvault.sh"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ROTATE] $1"
}

# Key Vault is only contacted for real runs; a dry run prints the plan instead.
kv_login_unless_dry_run() {
    [[ "${DRY_RUN}" == "true" ]] && return 0
    kv_require_login
}

kv_get_secret_unless_dry_run() {
    if [[ "${DRY_RUN}" == "true" ]]; then
        printf 'DRY_RUN_PUBLIC_KEY_BODY'
        return 0
    fi
    kv_get_secret "$1"
}

trap kv_cleanup EXIT

# ---------------------------------------------------------------------------
# Resolve the target principal
# ---------------------------------------------------------------------------
case "${TARGET_USER}" in
    synapse)
        SF_USER="${SF_SVC_SYNAPSE_USER}"
        KV_PRIVATE="${KV_SECRET_SYNAPSE_PRIVATE_KEY}"
        KV_PUBLIC="${KV_SECRET_SYNAPSE_PUBLIC_KEY}"
        KV_PASSPHRASE="${KV_SECRET_SYNAPSE_KEY_PASSPHRASE}"
        ;;
    loader)
        SF_USER="${SF_SVC_LOADER_USER}"
        KV_PRIVATE="${KV_SECRET_LOADER_PRIVATE_KEY}"
        KV_PUBLIC="${KV_SECRET_LOADER_PUBLIC_KEY}"
        KV_PASSPHRASE="${KV_SECRET_LOADER_KEY_PASSPHRASE}"
        ;;
    admin)
        SF_USER="SVC_RB_ADMIN_${SF_ENV}"
        KV_PRIVATE="${KV_SECRET_ADMIN_PRIVATE_KEY}"
        KV_PUBLIC="snowflake-svc-admin-public-key"
        KV_PASSPHRASE="${KV_SECRET_ADMIN_KEY_PASSPHRASE}"
        ;;
    *)
        echo "Usage: $0 --user synapse|loader|admin --phase generate|publish|cutover|retire [--dry-run]"
        exit 1
        ;;
esac

run_snowsql() {
    local statement="$1"

    if [[ "${DRY_RUN}" == "true" ]]; then
        log "(dry run) ${statement}"
        return 0
    fi

    local admin_key admin_pass
    admin_key="$(kv_get_private_key "${KV_SECRET_ADMIN_PRIVATE_KEY}")"
    admin_pass="$(kv_get_secret "${KV_SECRET_ADMIN_KEY_PASSPHRASE}")"

    SNOWSQL_PRIVATE_KEY_PASSPHRASE="${admin_pass}" \
    snowsql \
        --accountname "${SF_ACCOUNT}" \
        --username "SVC_RB_ADMIN_${SF_ENV}" \
        --private-key-path "${admin_key}" \
        --authenticator SNOWFLAKE_JWT \
        --rolename USERADMIN \
        --warehousename "${SF_WH_ELT}" \
        --option exit_on_error=true \
        --option friendly=false \
        --query "${statement}"
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------
case "${PHASE}" in

    generate)
        log "Generating a new 2048-bit RSA key pair for ${SF_USER}"
        WORK_DIR="$(mktemp -d)"
        chmod 700 "${WORK_DIR}"
        NEW_PASSPHRASE="$(openssl rand -base64 32)"

        openssl genrsa 2048 2>/dev/null \
            | openssl pkcs8 -topk8 -v2 des3 -inform PEM \
                -passout "pass:${NEW_PASSPHRASE}" \
                -out "${WORK_DIR}/new_key.p8"
        openssl rsa -in "${WORK_DIR}/new_key.p8" \
            -passin "pass:${NEW_PASSPHRASE}" \
            -pubout -out "${WORK_DIR}/new_key.pub"

        # Strip the PEM header/footer: Snowflake's RSA_PUBLIC_KEY takes the body.
        PUB_BODY="$(grep -v 'PUBLIC KEY' "${WORK_DIR}/new_key.pub" | tr -d '\n')"

        if [[ "${DRY_RUN}" == "true" ]]; then
            log "(dry run) would store new key versions in ${AZ_KEY_VAULT_NAME}"
        else
            kv_require_login
            az keyvault secret set --vault-name "${AZ_KEY_VAULT_NAME}" \
                --name "${KV_PRIVATE}-pending" --file "${WORK_DIR}/new_key.p8" \
                --output none
            az keyvault secret set --vault-name "${AZ_KEY_VAULT_NAME}" \
                --name "${KV_PASSPHRASE}-pending" --value "${NEW_PASSPHRASE}" \
                --output none
            az keyvault secret set --vault-name "${AZ_KEY_VAULT_NAME}" \
                --name "${KV_PUBLIC}-pending" --value "${PUB_BODY}" \
                --output none
            log "Pending key material stored in Key Vault"
        fi

        shred -u "${WORK_DIR}/new_key.p8" 2>/dev/null || rm -f "${WORK_DIR}/new_key.p8"
        rm -rf "${WORK_DIR}"
        unset NEW_PASSPHRASE
        ;;

    publish)
        log "Publishing the pending public key to slot 2 of ${SF_USER}"
        kv_login_unless_dry_run
        PUB_BODY="$(kv_get_secret_unless_dry_run "${KV_PUBLIC}-pending")"
        run_snowsql "ALTER USER ${SF_USER} SET RSA_PUBLIC_KEY_2 = '${PUB_BODY}';"
        log "Both keys are now accepted; verify connectivity before cutover"
        ;;

    cutover)
        log "Promoting the pending private key to current for ${SF_USER}"
        kv_login_unless_dry_run
        if [[ "${DRY_RUN}" == "true" ]]; then
            log "(dry run) would promote -pending secrets to current"
        else
            PENDING_KEY_FILE="$(kv_get_private_key "${KV_PRIVATE}-pending")"
            PENDING_PASS="$(kv_get_secret "${KV_PASSPHRASE}-pending")"
            PENDING_PUB="$(kv_get_secret "${KV_PUBLIC}-pending")"

            az keyvault secret set --vault-name "${AZ_KEY_VAULT_NAME}" \
                --name "${KV_PRIVATE}" --file "${PENDING_KEY_FILE}" --output none
            az keyvault secret set --vault-name "${AZ_KEY_VAULT_NAME}" \
                --name "${KV_PASSPHRASE}" --value "${PENDING_PASS}" --output none
            az keyvault secret set --vault-name "${AZ_KEY_VAULT_NAME}" \
                --name "${KV_PUBLIC}" --value "${PENDING_PUB}" --output none
            unset PENDING_PASS
            log "Consumers now resolve the new key on their next Key Vault read"
        fi
        ;;

    retire)
        log "Retiring the previous key for ${SF_USER}"
        kv_login_unless_dry_run
        PUB_BODY="$(kv_get_secret_unless_dry_run "${KV_PUBLIC}")"
        run_snowsql "ALTER USER ${SF_USER} SET RSA_PUBLIC_KEY = '${PUB_BODY}';"
        run_snowsql "ALTER USER ${SF_USER} UNSET RSA_PUBLIC_KEY_2;"

        if [[ "${DRY_RUN}" == "true" ]]; then
            log "(dry run) would delete the -pending secrets"
        else
            for pending in "${KV_PRIVATE}-pending" "${KV_PASSPHRASE}-pending" "${KV_PUBLIC}-pending"; do
                az keyvault secret delete --vault-name "${AZ_KEY_VAULT_NAME}" \
                    --name "${pending}" --output none || true
            done
        fi
        log "Rotation complete for ${SF_USER}"
        ;;

    *)
        echo "Usage: $0 --user synapse|loader|admin --phase generate|publish|cutover|retire [--dry-run]"
        exit 1
        ;;
esac
