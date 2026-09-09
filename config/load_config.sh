#!/bin/bash
# =============================================================================
# Pipeline Configuration Loader
# =============================================================================
# Parses config/pipeline_config.cfg as inert KEY=VALUE data. The config file
# is never executed as shell: values are taken literally (no expansion, no
# command substitution) and validated before being exported.
#
# Usage (from an orchestrator):
#   source "${SCRIPT_DIR}/../config/load_config.sh"
# =============================================================================

load_pipeline_config() {
    local cfg_file="${1:?config file path required}"

    if [ ! -f "${cfg_file}" ]; then
        echo "ERROR: config file not found: ${cfg_file}" >&2
        return 1
    fi

    # Refuse configs that another principal could have tampered with.
    local owner perms
    owner=$(stat -c '%u' "${cfg_file}")
    perms=$(stat -c '%a' "${cfg_file}")
    if [ "${owner}" != "$(id -u)" ] && [ "${owner}" != "0" ]; then
        echo "ERROR: config file ${cfg_file} must be owned by the ETL service account or root" >&2
        return 1
    fi
    if [ $(( 8#${perms} & 8#022 )) -ne 0 ]; then
        echo "ERROR: config file ${cfg_file} is group/world writable (mode ${perms})" >&2
        return 1
    fi

    local -A cfg=()
    local line key value lineno=0
    while IFS= read -r line || [ -n "${line}" ]; do
        lineno=$((lineno + 1))
        line="${line%$'\r'}"
        # Skip blanks and comments
        [[ "${line}" =~ ^[[:space:]]*(#.*)?$ ]] && continue

        if [[ ! "${line}" =~ ^([A-Z][A-Z0-9_]*)=(.*)$ ]]; then
            echo "ERROR: ${cfg_file}:${lineno}: expected KEY=VALUE, got: ${line}" >&2
            return 1
        fi
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"

        # Strip trailing comment and surrounding whitespace
        value="${value%%[[:space:]]#*}"
        value="${value#"${value%%[![:space:]]*}"}"
        value="${value%"${value##*[![:space:]]}"}"
        # Strip one pair of matching quotes; contents are still literal
        if [[ "${value}" =~ ^\"(.*)\"$ ]] || [[ "${value}" =~ ^\'(.*)\'$ ]]; then
            value="${BASH_REMATCH[1]}"
        fi
        # Reject anything that looks like shell syntax
        if [[ "${value}" == *[\$\`\;\|\&\<\>\(\)\{\}\\]* ]] || [[ "${value}" == *$'\n'* ]]; then
            echo "ERROR: ${cfg_file}:${lineno}: illegal characters in value for ${key}" >&2
            return 1
        fi
        if [ -n "${cfg[${key}]+x}" ]; then
            echo "ERROR: ${cfg_file}:${lineno}: duplicate key ${key}" >&2
            return 1
        fi
        cfg["${key}"]="${value}"
    done < "${cfg_file}"

    local -a required=(
        TD_SERVER TD_USERNAME TD_LOGMECH
        DB_CORE DB_TXN DB_STG DB_DP
        SAS_HOME SAS_CONFIG SAS_AUTOEXEC
        PIPELINE_HOME
        LOOKBACK_MONTHS RISK_SCORE_THRESHOLD
        LOG_LEVEL
    )
    for key in "${required[@]}"; do
        if [ -z "${cfg[${key}]+x}" ]; then
            echo "ERROR: ${cfg_file}: missing required key ${key}" >&2
            return 1
        fi
    done

    # -- Validation -----------------------------------------------------------
    local hostname_re='^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$'
    local ident_re='^[A-Za-z_][A-Za-z0-9_]*$'
    local abspath_re='^/[A-Za-z0-9._/-]*$'
    local int_re='^[0-9]+$'

    if [[ ! "${cfg[TD_SERVER]}" =~ ${hostname_re} ]]; then
        echo "ERROR: TD_SERVER is not a valid hostname: ${cfg[TD_SERVER]}" >&2; return 1
    fi
    if [[ ! "${cfg[TD_SERVER]}" =~ \.corp\.bankdemo\.com$ ]]; then
        echo "ERROR: TD_SERVER must be a *.corp.bankdemo.com host: ${cfg[TD_SERVER]}" >&2; return 1
    fi
    if [[ ! "${cfg[TD_USERNAME]}" =~ ${ident_re} ]]; then
        echo "ERROR: TD_USERNAME is not a valid identifier: ${cfg[TD_USERNAME]}" >&2; return 1
    fi
    if [[ ! "${cfg[TD_USERNAME]}" =~ ^svc_ ]]; then
        echo "ERROR: TD_USERNAME must be a service account (svc_*): ${cfg[TD_USERNAME]}" >&2; return 1
    fi
    case "${cfg[TD_LOGMECH]}" in
        LDAP|TD2|KRB5) ;;
        *) echo "ERROR: TD_LOGMECH must be one of LDAP|TD2|KRB5: ${cfg[TD_LOGMECH]}" >&2; return 1 ;;
    esac
    for key in DB_CORE DB_TXN DB_STG DB_DP; do
        if [[ ! "${cfg[${key}]}" =~ ${ident_re} ]]; then
            echo "ERROR: ${key} is not a valid database name: ${cfg[${key}]}" >&2; return 1
        fi
    done
    for key in SAS_HOME SAS_CONFIG SAS_AUTOEXEC PIPELINE_HOME; do
        if [[ ! "${cfg[${key}]}" =~ ${abspath_re} ]]; then
            echo "ERROR: ${key} must be an absolute path: ${cfg[${key}]}" >&2; return 1
        fi
    done
    if [[ ! "${cfg[LOOKBACK_MONTHS]}" =~ ${int_re} ]] \
        || [ "${cfg[LOOKBACK_MONTHS]}" -lt 1 ] || [ "${cfg[LOOKBACK_MONTHS]}" -gt 120 ]; then
        echo "ERROR: LOOKBACK_MONTHS must be an integer between 1 and 120: ${cfg[LOOKBACK_MONTHS]}" >&2; return 1
    fi
    if [[ ! "${cfg[RISK_SCORE_THRESHOLD]}" =~ ${int_re} ]] \
        || [ "${cfg[RISK_SCORE_THRESHOLD]}" -lt 300 ] || [ "${cfg[RISK_SCORE_THRESHOLD]}" -gt 850 ]; then
        echo "ERROR: RISK_SCORE_THRESHOLD must be an integer between 300 and 850: ${cfg[RISK_SCORE_THRESHOLD]}" >&2; return 1
    fi
    case "${cfg[LOG_LEVEL]}" in
        DEBUG|INFO|WARN|ERROR) ;;
        *) echo "ERROR: LOG_LEVEL must be one of DEBUG|INFO|WARN|ERROR: ${cfg[LOG_LEVEL]}" >&2; return 1 ;;
    esac

    # -- Export ---------------------------------------------------------------
    for key in "${required[@]}"; do
        export "${key}=${cfg[${key}]}"
    done

    # Derived values are computed here, never read from the config file
    export SAS_BATCH="${SAS_HOME}/sas"
    export BTEQ_DIR="${PIPELINE_HOME}/bteq"
    export SAS_DIR="${PIPELINE_HOME}/sas"
    export LOG_DIR="${PIPELINE_HOME}/logs"
    export ARCHIVE_DIR="${PIPELINE_HOME}/archive"
    export RUN_DATE
    export RUN_TIMESTAMP
    RUN_DATE=$(date +%Y%m%d)
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
}

_LOADER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
load_pipeline_config "${_LOADER_DIR}/pipeline_config.cfg" || exit 1
unset _LOADER_DIR
