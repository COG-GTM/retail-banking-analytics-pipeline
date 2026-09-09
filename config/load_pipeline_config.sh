#!/bin/bash
# =============================================================================
# Pipeline Configuration Loader
# =============================================================================
# Reads config/pipeline_config.cfg as plain KEY=VALUE data (never executed as
# shell), accepts only allowlisted keys, validates every value, and exports
# the result. Derived values (SAS_BATCH, *_DIR, RUN_DATE, RUN_TIMESTAMP) are
# computed here rather than read from the file.
#
# Usage (from an orchestrator):
#   source "${SCRIPT_DIR}/../config/load_pipeline_config.sh"
#   load_pipeline_config "${SCRIPT_DIR}/../config/pipeline_config.cfg"
# =============================================================================

_cfg_die() {
    echo "[CONFIG ERROR] $1" >&2
    exit 78   # EX_CONFIG
}

# Regex per allowlisted key. Anything not listed here is rejected.
_cfg_pattern() {
    case "$1" in
        TD_SERVER)            echo '^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$' ;;
        TD_USERNAME)          echo '^[A-Za-z_][A-Za-z0-9_]{0,63}$' ;;
        TD_LOGMECH)           echo '^(TD2|LDAP|KRB5|TDNEGO)$' ;;
        DB_CORE|DB_TXN|DB_STG|DB_DP)
                              echo '^[A-Za-z_][A-Za-z0-9_]{0,127}$' ;;
        SAS_HOME|SAS_CONFIG|SAS_AUTOEXEC|PIPELINE_HOME)
                              echo '^/([A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+$' ;;
        LOOKBACK_MONTHS)      echo '^([1-9]|[1-5][0-9]|60)$' ;;
        RISK_SCORE_THRESHOLD) echo '^(30[0-9]|[4-7][0-9][0-9]|8[0-4][0-9]|850)$' ;;
        LOG_LEVEL)            echo '^(DEBUG|INFO|WARN|ERROR)$' ;;
        *)                    return 1 ;;
    esac
}

_cfg_check_perms() {
    local file="$1" mode owner
    [ -f "${file}" ] || _cfg_die "config file not found: ${file}"
    [ -L "${file}" ] && _cfg_die "config file must not be a symlink: ${file}"
    mode=$(stat -c '%a' "${file}") || _cfg_die "cannot stat ${file}"
    owner=$(stat -c '%u' "${file}") || _cfg_die "cannot stat ${file}"
    if [ "${owner}" != "$(id -u)" ] && [ "${owner}" != "0" ]; then
        _cfg_die "config file ${file} must be owned by the pipeline user or root"
    fi
    # Reject group/world write (octal digits 2 and 3 of a 3-4 digit mode)
    if [ $(( 8#${mode} & 8#022 )) -ne 0 ]; then
        _cfg_die "config file ${file} is group/world writable (mode ${mode}); expected 0640 or stricter"
    fi
}

load_pipeline_config() {
    local file="$1"
    local -A seen=()
    local line key value pattern lineno=0
    local env_td_username="${TD_USERNAME:-}"

    _cfg_check_perms "${file}"

    while IFS= read -r line || [ -n "${line}" ]; do
        lineno=$((lineno + 1))
        # Strip leading/trailing whitespace and CR
        line="${line%$'\r'}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [ -z "${line}" ] && continue
        [ "${line:0:1}" = "#" ] && continue

        [[ "${line}" =~ ^([A-Z][A-Z0-9_]*)=(.*)$ ]] \
            || _cfg_die "${file}:${lineno}: expected KEY=VALUE, got: ${line}"
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"

        # Allow a single pair of surrounding double quotes; nothing inside is expanded
        if [[ "${value}" =~ ^\"(.*)\"$ ]]; then
            value="${BASH_REMATCH[1]}"
        fi

        pattern=$(_cfg_pattern "${key}") \
            || _cfg_die "${file}:${lineno}: key '${key}' is not an allowed configuration key"
        [ -n "${seen[${key}]:-}" ] && _cfg_die "${file}:${lineno}: duplicate key '${key}'"
        seen["${key}"]=1

        [[ "${value}" =~ ${pattern} ]] \
            || _cfg_die "${file}:${lineno}: invalid value for ${key}"

        printf -v "${key}" '%s' "${value}"
        export "${key}"
    done < "${file}"

    local required
    for required in TD_SERVER TD_LOGMECH DB_CORE DB_TXN DB_STG DB_DP \
                    SAS_HOME SAS_CONFIG SAS_AUTOEXEC PIPELINE_HOME \
                    LOOKBACK_MONTHS RISK_SCORE_THRESHOLD LOG_LEVEL; do
        [ -n "${seen[${required}]:-}" ] || _cfg_die "${file}: missing required key ${required}"
    done

    # Scheduler environment overrides the file; validate either way
    export TD_USERNAME="${env_td_username:-${TD_USERNAME:-svc_etl_pipeline}}"
    [[ "${TD_USERNAME}" =~ $(_cfg_pattern TD_USERNAME) ]] || _cfg_die "invalid TD_USERNAME"

    # Derived values - never read from the file
    export SAS_BATCH="${SAS_HOME}/sas"
    export BTEQ_DIR="${PIPELINE_HOME}/bteq"
    export SAS_DIR="${PIPELINE_HOME}/sas"
    export LOG_DIR="${PIPELINE_HOME}/logs"
    export ARCHIVE_DIR="${PIPELINE_HOME}/archive"
    RUN_DATE=$(date +%Y%m%d);        export RUN_DATE
    RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S); export RUN_TIMESTAMP
}
