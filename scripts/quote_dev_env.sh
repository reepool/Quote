#!/usr/bin/env bash
# Source in a development terminal, or execute with a command and its arguments.

_quote_dev_environment() {
    local quote_tmp_root
    quote_tmp_root="/home/python/Quote/data/QuoteBak"

    # QuoteBak is an NFS mount; do not fill the local disk if it is unmounted.
    if ! mountpoint -q -- "$quote_tmp_root"; then
        printf 'Quote development temporary directory is not mounted: %s\n' "$quote_tmp_root" >&2
        return 1
    fi

    if ! mkdir -p -- "$quote_tmp_root/cache" "$quote_tmp_root/tests/coverage"; then
        printf 'Quote development temporary directory is unavailable: %s\n' "$quote_tmp_root" >&2
        return 1
    fi
    if [[ ! -w "$quote_tmp_root" || ! -x "$quote_tmp_root" ]]; then
        printf 'Quote development temporary directory is not writable: %s\n' "$quote_tmp_root" >&2
        return 1
    fi

    export TMPDIR="$quote_tmp_root"
    export TMP="$quote_tmp_root"
    export TEMP="$quote_tmp_root"
    export SQLITE_TMPDIR="$quote_tmp_root"
    export PYTEST_DEBUG_TEMPROOT="$quote_tmp_root"
    export XDG_CACHE_HOME="$quote_tmp_root/cache"
    export PIP_CACHE_DIR="$XDG_CACHE_HOME/pip"
    export npm_config_cache="$XDG_CACHE_HOME/npm"
    export RUFF_CACHE_DIR="$XDG_CACHE_HOME/ruff"
    export MYPY_CACHE_DIR="$XDG_CACHE_HOME/mypy"
    export MPLCONFIGDIR="$XDG_CACHE_HOME/matplotlib"
    # Keep existing installed bytecode readable without generating development
    # bytecode or doing a network cache lookup for every Python import.
    unset PYTHONPYCACHEPREFIX
    export PYTHONDONTWRITEBYTECODE=1
    export NODE_COMPILE_CACHE="$XDG_CACHE_HOME/node-compile"
    export COVERAGE_FILE="$quote_tmp_root/tests/coverage/.coverage"
    export QUOTE_DEV_LOG_DIR="$quote_tmp_root/logs"
}

if ! _quote_dev_environment; then
    unset -f _quote_dev_environment
    return 1 2>/dev/null || exit 1
fi
unset -f _quote_dev_environment

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    if [[ "$#" -eq 0 ]]; then
        printf 'Usage: bash %s <command> [args...]\nOr: source %s\n' "$0" "$0" >&2
        exit 2
    fi
    exec "$@"
fi
