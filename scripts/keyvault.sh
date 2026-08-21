#!/bin/bash
# =============================================================================
# Azure Key Vault Helpers - Retail Banking Customer Analytics
# Ticket: MBA-2203 (TICKET-02)
# =============================================================================
# Sourced by the Snowflake admin runner, the rotation script and the migrated
# pipeline entrypoints. Provides the single supported way to obtain a
# credential: fetch it from Azure Key Vault at runtime.
#
# Authentication to Key Vault itself uses the caller's Azure identity:
#   - Synapse / Azure VM : workspace managed identity (az login --identity)
#   - Engineer workstation: az login (Entra interactive, MFA enforced)
# No Key Vault credential is ever stored on disk or in this repository.
#
# Usage:
#   source scripts/keyvault.sh
#   kv_require_login
#   passphrase="$(kv_get_secret "${KV_SECRET_SYNAPSE_KEY_PASSPHRASE}")"
#   key_file="$(kv_get_private_key "${KV_SECRET_SYNAPSE_PRIVATE_KEY}")"
#   ...
#   kv_cleanup            # shred every materialised key file
# =============================================================================

set -uo pipefail

KV_TMP_FILES=()

kv_log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [keyvault] $1" >&2
}

# ---------------------------------------------------------------------------
# Ensure the Azure CLI is present and an identity is signed in.
# ---------------------------------------------------------------------------
kv_require_login() {
    if ! command -v az >/dev/null 2>&1; then
        kv_log "ERROR: azure-cli (az) is not installed"
        return 1
    fi

    if ! az account show >/dev/null 2>&1; then
        kv_log "No active Azure session; attempting managed identity login"
        if ! az login --identity >/dev/null 2>&1; then
            kv_log "ERROR: unable to authenticate to Azure. Run 'az login' first."
            return 1
        fi
    fi

    if [[ -z "${AZ_KEY_VAULT_NAME:-}" ]]; then
        kv_log "ERROR: AZ_KEY_VAULT_NAME is not set (source config/snowflake_config.cfg)"
        return 1
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Echo a secret value. Callers must keep the value in a shell variable only -
# never write it to a log, a config file or a repository artefact.
# ---------------------------------------------------------------------------
kv_get_secret() {
    local secret_name="$1"
    local value

    if ! value="$(az keyvault secret show \
                    --vault-name "${AZ_KEY_VAULT_NAME}" \
                    --name "${secret_name}" \
                    --query value -o tsv 2>/dev/null)"; then
        kv_log "ERROR: could not read secret '${secret_name}' from ${AZ_KEY_VAULT_NAME}"
        return 1
    fi

    if [[ -z "${value}" ]]; then
        kv_log "ERROR: secret '${secret_name}' is empty in ${AZ_KEY_VAULT_NAME}"
        return 1
    fi

    printf '%s' "${value}"
}

# ---------------------------------------------------------------------------
# Materialise a PEM private key into a 0600 file and echo its path. The file is
# tracked so kv_cleanup can shred it when the caller finishes.
# ---------------------------------------------------------------------------
kv_get_private_key() {
    local secret_name="$1"
    local key_file

    key_file="$(mktemp -t "sf_key_XXXXXXXX.p8")"
    chmod 600 "${key_file}"
    KV_TMP_FILES+=("${key_file}")

    if ! kv_get_secret "${secret_name}" > "${key_file}"; then
        rm -f "${key_file}"
        return 1
    fi

    printf '%s' "${key_file}"
}

# ---------------------------------------------------------------------------
# Shred every key file materialised in this shell.
# ---------------------------------------------------------------------------
kv_cleanup() {
    local f
    for f in "${KV_TMP_FILES[@]:-}"; do
        [[ -f "${f}" ]] || continue
        if command -v shred >/dev/null 2>&1; then
            shred -u "${f}" 2>/dev/null || rm -f "${f}"
        else
            rm -f "${f}"
        fi
    done
    KV_TMP_FILES=()
}
