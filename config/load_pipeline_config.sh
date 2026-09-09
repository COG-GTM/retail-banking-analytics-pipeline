#!/bin/bash
# =============================================================================
# Pipeline Configuration Loader
# =============================================================================
# Reads config/pipeline_config.cfg as plain KEY=VALUE data. The file is never
# sourced or eval'd, so nothing in it can execute in the pipeline's context.
#
# Rules enforced on the config file:
#   - regular file, owned by the invoking user or root, not group/world-writable
#   - one KEY=VALUE per line (blank lines and '#' comments allowed)
#   - KEY must be in the allow-list below; VALUE may be optionally quoted and
#     must not contain shell metacharacters ($ ` \ ; | & < > ( ) { } newline)
#
# Derived values (paths, timestamps) are computed here, not in the config.
#
# Usage:  source "${SCRIPT_DIR}/../config/load_pipeline_config.sh"
# =============================================================================

PIPELINE_CONFIG_FILE="${PIPELINE_CONFIG_FILE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/pipeline_config.cfg}"

_cfg_die() {
    echo "[CONFIG] ERROR: $1" >&2
    exit 1
}

_cfg_check_file() {
    local f="$1" mode uid
    [ -f "${f}" ] || _cfg_die "config file not found: ${f}"
    [ -L "${f}" ] && _cfg_die "config file must not be a symlink: ${f}"
    mode="$(stat -c '%a' "${f}")"
    uid="$(stat -c '%u' "${f}")"
    if [ "${uid}" != "$(id -u)" ] && [ "${uid}" != "0" ]; then
        _cfg_die "config file must be owned by the pipeline user or root: ${f}"
    fi
    if [ $(( 8#${mode} & 8#022 )) -ne 0 ]; then
        _cfg_die "config file must not be group/world-writable (mode ${mode}): ${f}"
    fi
}

_cfg_allowed_key() {
    case "$1" in
        TD_SERVER|TD_USERNAME|TD_LOGMECH|\
        DB_CORE|DB_TXN|DB_STG|DB_DP|\
        SAS_HOME|SAS_CONFIG|SAS_AUTOEXEC|\
        PIPELINE_HOME|\
        LOOKBACK_MONTHS|RISK_SCORE_THRESHOLD|\
        LOG_LEVEL) return 0 ;;
        *) return 1 ;;
    esac
}

_cfg_load() {
    local f="$1" line key value lineno=0
    _cfg_check_file "${f}"

    while IFS= read -r line || [ -n "${line}" ]; do
        lineno=$(( lineno + 1 ))
        line="${line%%#*}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [ -z "${line}" ] && continue

        if [[ ! "${line}" =~ ^([A-Z][A-Z0-9_]*)=(.*)$ ]]; then
            _cfg_die "line ${lineno}: expected KEY=VALUE"
        fi
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"

        _cfg_allowed_key "${key}" || _cfg_die "line ${lineno}: unknown key '${key}'"

        if [[ "${value}" =~ ^\"(.*)\"$ ]] || [[ "${value}" =~ ^\'(.*)\'$ ]]; then
            value="${BASH_REMATCH[1]}"
        fi
        if [[ "${value}" == *[\$\`\\\;\|\&\<\>\(\)\{\}\"\']* ]]; then
            _cfg_die "line ${lineno}: value for '${key}' contains shell metacharacters"
        fi

        case "${key}" in
            LOOKBACK_MONTHS|RISK_SCORE_THRESHOLD)
                [[ "${value}" =~ ^[0-9]+$ ]] || _cfg_die "line ${lineno}: '${key}' must be an integer" ;;
            LOG_LEVEL)
                case "${value}" in DEBUG|INFO|WARN|ERROR) ;; *) _cfg_die "line ${lineno}: invalid LOG_LEVEL" ;; esac ;;
            SAS_HOME|SAS_CONFIG|SAS_AUTOEXEC|PIPELINE_HOME)
                [[ "${value}" == /* ]] || _cfg_die "line ${lineno}: '${key}' must be an absolute path" ;;
        esac

        # TD_USERNAME may be pre-set by the scheduler; the file provides the default.
        if [ "${key}" = "TD_USERNAME" ] && [ -n "${TD_USERNAME:-}" ]; then
            continue
        fi
        printf -v "${key}" '%s' "${value}"
        export "${key}"
    done < "${f}"

    local required
    for required in TD_SERVER TD_USERNAME TD_LOGMECH DB_CORE DB_TXN DB_STG DB_DP \
                    SAS_HOME SAS_CONFIG SAS_AUTOEXEC PIPELINE_HOME \
                    LOOKBACK_MONTHS RISK_SCORE_THRESHOLD LOG_LEVEL; do
        [ -n "${!required:-}" ] || _cfg_die "missing required key '${required}'"
    done
}

_cfg_load "${PIPELINE_CONFIG_FILE}"

# -- Derived values -------------------------------------------------------------
export SAS_BATCH="${SAS_HOME}/sas"
export BTEQ_DIR="${PIPELINE_HOME}/bteq"
export SAS_DIR="${PIPELINE_HOME}/sas"
export LOG_DIR="${PIPELINE_HOME}/logs"
export ARCHIVE_DIR="${PIPELINE_HOME}/archive"

# RUN_* are shared across the orchestrator and sub-pipelines within one run.
export RUN_DATE="${RUN_DATE:-$(date +%Y%m%d)}"
export RUN_TIMESTAMP="${RUN_TIMESTAMP:-$(date +%Y%m%d_%H%M%S)}"

unset -f _cfg_die _cfg_check_file _cfg_allowed_key _cfg_load
